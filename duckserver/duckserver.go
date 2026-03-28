// Package duckserver proivdes HTTP server of DuckHouse.
package duckserver

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/koron-go/ctxsrv"
	"github.com/koron-go/daemonic/hupfile"
	"github.com/koron-go/daemonic/pidfile"
	"github.com/koron/duckhouse/internal/accesslog"
	"github.com/koron/duckhouse/internal/authn"
	"github.com/koron/duckhouse/internal/conndb"
	"github.com/koron/duckhouse/internal/duckdbinit"
	"github.com/koron/duckhouse/internal/fileserver"
	"github.com/koron/duckhouse/internal/httperror"
)

const (
	AuthnIDHeader      = "Duckhouse-Authnid"
	ConnectionIDHeader = "Duckhouse-Connectionid"
	DurationHeader     = "Duckhouse-Duration"

	defaultFormat = "csv"
)

type Config struct {
	EnableDebugLog bool

	MaxDB   int
	Address string

	PIDFile         string
	AccessLogFile   string
	AccessLogFormat string

	AuthnFile string
	NoAuthz   bool

	DBThreads        int
	DBMemoryLimiit   string
	DBHomeDir        string
	DBMaxTempDirSize string
	DBExternalAccess bool
	DBLockConfig     bool
	DBInitQuery      string

	UIResourceDir string
}

func getwd() string {
	wd, err := os.Getwd()
	if err != nil {
		return ""
	}
	return wd
}

func DefaultConfig() Config {
	return Config{
		MaxDB:            20,
		Address:          "localhost:9998",
		AccessLogFormat:  "text",
		DBThreads:        1,
		DBMemoryLimiit:   "1GiB",
		DBHomeDir:        filepath.Join(getwd(), ".duckhouse"),
		DBMaxTempDirSize: "10GiB",
		DBExternalAccess: true,
		DBLockConfig:     true,
	}
}

type Server struct {
	logger       *slog.Logger
	accessLogger *slog.Logger

	address         string
	pidFile         string
	accessLogFile   string
	accessLogFormat logFormat

	authenticator *authn.Authenticator
	withoutAuthz  bool

	dbSharedDir   string
	dbPrivateRoot string
	dbSettings    duckdbinit.Settings
	dbInitQuery   string

	connManager *conndb.Manager
}

func New(c Config) (*Server, error) {
	homedir, err := filepath.Abs(c.DBHomeDir)
	if err != nil {
		return nil, fmt.Errorf("failed to detemine DBHomeDir: %w", err)
	}

	srv := Server{
		address:       c.Address,
		pidFile:       c.PIDFile,
		accessLogFile: c.AccessLogFile,
		dbSharedDir:   filepath.Join(homedir, "shared"),
		dbPrivateRoot: filepath.Join(homedir, "private"),
		dbSettings: duckdbinit.Settings{
			HomeDir:              homedir,
			Threads:              c.DBThreads,
			MemoryLimit:          c.DBMemoryLimiit,
			ExtensionDir:         filepath.Join(homedir, "extensions"),
			SecretDir:            filepath.Join(homedir, "stored_secrets"),
			TempDir:              filepath.Join(homedir, "tmp"),
			MaxTempDirSize:       c.DBMaxTempDirSize,
			EnableExternalAccess: c.DBExternalAccess,
			LockConfig:           c.DBLockConfig,
		},
		dbInitQuery: c.DBInitQuery,
	}

	srv.logger = slog.Default()
	if c.EnableDebugLog {
		slog.SetLogLoggerLevel(slog.LevelDebug)
	}

	lf, err := parseLogFormat(c.AccessLogFormat)
	if err != nil {
		return nil, err
	}
	srv.accessLogFormat = lf

	// TODO: UIResourceDir

	if c.AuthnFile != "" {
		a, err := authn.LoadFile(c.AuthnFile)
		if err != nil {
			return nil, err
		}
		srv.authenticator = a
	}
	if c.NoAuthz {
		if srv.authenticator == nil {
			// FIXME: brush up message
			return nil, errors.New("-noauthz need to be used with -authnfile")
		}
		srv.withoutAuthz = true
	}

	if c.DBInitQuery != "" {
		if strings.HasPrefix(c.DBInitQuery, "@") {
			b, err := os.ReadFile(c.DBInitQuery[1:])
			if err != nil {
				return nil, fmt.Errorf("failed to read init query: %w", err)
			}
			srv.dbInitQuery = string(b)
		} else {
			srv.dbInitQuery = c.DBInitQuery
		}
	}

	// Setup DB connection manager
	srv.connManager = &conndb.Manager{
		MaxDB:  c.MaxDB,
		Opener: conndb.OpenerFunc(srv.newDuckDB),
		Closer: conndb.CloserFunc(srv.closeDuckDB),
	}

	return &srv, nil
}

type logFormat int

const (
	textLog logFormat = iota + 1
	jsonLog
)

func parseLogFormat(s string) (logFormat, error) {
	switch strings.ToLower(s) {
	case "text":
		return textLog, nil
	case "json":
		return jsonLog, nil
	default:
		return 0, fmt.Errorf("unsupported log format: %q", s)
	}
}

func (srv *Server) Serve(ctx context.Context) error {
	// Preparement: check database configuration.
	err := srv.checkDB(ctx)
	if err != nil {
		return err
	}

	// Setup access logger
	var logw io.Writer = os.Stdout
	if srv.accessLogFile != "" {
		w, err := hupfile.New(srv.accessLogFile)
		if err != nil {
			return fmt.Errorf("failed to open access log file: %w", err)
		}
		logw = w
		defer w.Close()
	}
	switch srv.accessLogFormat {
	default:
		fallthrough
	case textLog:
		srv.accessLogger = slog.New(slog.NewTextHandler(logw, nil))
	case jsonLog:
		srv.accessLogger = slog.New(slog.NewJSONHandler(logw, nil))
	}

	// Set PID file
	if srv.pidFile != "" {
		err := pidfile.Write(srv.pidFile)
		if err != nil {
			return fmt.Errorf("failed to create PID file: %w", err)
		}
		defer pidfile.Close()
	}

	// Start server
	httpsrv := &http.Server{
		Addr:        srv.address,
		Handler:     srv.newDuckhouseHandler(),
		ConnContext: srv.connManager.ConnContext,
		ConnState:   srv.connManager.ConnState,
	}
	srvctx, cancel := signal.NotifyContext(ctx, os.Interrupt, syscall.SIGTERM)
	defer cancel()
	srv.logger.Info("listening on", "addr", httpsrv.Addr)
	return ctxsrv.HTTP(httpsrv).WithShutdownTimeout(time.Minute).ServeWithContext(srvctx)
}

func (srv *Server) checkDB(ctx context.Context) error {
	db, err := srv.newDuckDB(ctx)
	if err != nil {
		return err
	}
	defer db.Close()
	return db.PingContext(ctx)
}

func (srv *Server) newDuckDB(ctx context.Context) (*sql.DB, error) {
	// Compose duckdbinit.Settings
	settings := srv.dbSettings
	if srv.dbSharedDir != "" {
		if err := os.MkdirAll(srv.dbSharedDir, 0777); err != nil {
			return nil, err
		}
		settings.AllowedDirectories = append(settings.AllowedDirectories, srv.dbSharedDir)
	}
	privateDir, err := srv.getPrivateDir(ctx, true)
	if err != nil {
		return nil, err
	}
	if privateDir != "" {
		settings.AllowedDirectories = append(settings.AllowedDirectories, privateDir)
	}
	// Prepare initQueries
	initQueries := make([]string, 0, 4)
	if srv.dbSharedDir != "" {
		initQueries = append(initQueries, fmt.Sprintf("CREATE MACRO public_dir(name) AS concat('%s', '/', name)", srv.dbSharedDir))
	}
	if privateDir != "" {
		initQueries = append(initQueries, fmt.Sprintf("CREATE MACRO private_dir(name) AS concat('%s', '/', name)", privateDir))
	}
	if srv.dbInitQuery != "" {
		initQueries = append(initQueries, srv.dbInitQuery)
	}
	if entry, ok := authn.AuthnEntry(ctx); ok && entry.InitQuery != "" {
		initQueries = append(initQueries, entry.InitQuery)
	}
	return duckdbinit.Open(ctx, settings, initQueries...)
}

func (srv *Server) closeDuckDB(ctx context.Context, db *sql.DB) error {
	privateDir, _ := srv.getPrivateDir(ctx, false)
	if privateDir != "" {
		if err := os.RemoveAll(privateDir); err != nil {
			srv.logger.Warn("failed to remove private directory", "dir", privateDir, "error", err)
		}
	}
	return db.Close()
}

func (srv *Server) getPrivateDir(ctx context.Context, makeDir bool) (string, error) {
	if srv.dbPrivateRoot == "" {
		return "", nil
	}
	connID, ok := srv.connManager.GetID(ctx)
	if !ok {
		slog.Debug("connection ID cannot be determined")
		return "", nil
	}
	privateDir := filepath.Join(srv.dbPrivateRoot, connID.String())
	if makeDir {
		if err := os.MkdirAll(privateDir, 0777); err != nil {
			return "", err
		}
	}
	return privateDir, nil
}

func (srv *Server) newDuckhouseHandler() http.Handler {
	mux := http.NewServeMux()
	// TODO: implement me.
	//mux.Handle("/{$}", errorAwareHandler(handleQuery))
	mux.Handle("GET /ping/{$}", errorAwareHandler(srv.handlePing))
	//mux.Handle("GET /status/connections/{$}", errorAwareHandler(handleStatusConnections))
	//mux.Handle("GET /status/queries/{$}", errorAwareHandler(handleStatusQueries))
	//mux.Handle("DELETE /status/queries/{queryID}", errorAwareHandler(handleInterruptQuery))
	//mux.Handle("/ui/", http.StripPrefix("/ui/", http.FileServerFS(uiFS)))
	if srv.dbSharedDir != "" {
		h := srv.authzChangeOperationHanlder(fileserver.New(srv.dbSharedDir))
		mux.Handle("/shared/", http.StripPrefix("/shared/", h))
	}

	// Install middlewares.
	var h http.Handler = mux
	h = accesslog.WrapHandler(srv.accessLogger, h)
	h = srv.authenticator.AuthenticateHandler(h)
	return h
}

func errorAwareHandler(handle func(http.ResponseWriter, *http.Request) error) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		err := handle(w, r)
		if err != nil {
			httperror.Write(w, err)
		}
	})
}

func (srv *Server) checkAuthz(w http.ResponseWriter, r *http.Request) error {
	if srv.authenticator == nil {
		return nil
	}
	if id, ok := authn.AuthnID(r.Context()); ok {
		// Insert AuthnID to response header.
		w.Header().Set(AuthnIDHeader, id.String())
		return nil
	}
	if srv.withoutAuthz {
		return nil
	}
	return httperror.New(401)
}

func (srv *Server) authzChangeOperationHanlder(handle http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case "GET", "HEAD", "OPTIONS", "PROPFIND":
			// no authz
		default:
			// Under authz control
			err := srv.checkAuthz(w, r)
			if err != nil {
				httperror.Write(w, err)
				return
			}
		}
		handle.ServeHTTP(w, r)
	})
}

func (srv *Server) handlePing(w http.ResponseWriter, r *http.Request) error {
	w.WriteHeader(200)
	w.Write([]byte("OK\r\n"))
	return nil
}
