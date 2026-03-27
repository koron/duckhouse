package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/duckdb/duckdb-go/v2"
	"github.com/koron-go/ctxsrv"
	"github.com/koron-go/daemonic/hupfile"
	"github.com/koron-go/daemonic/pidfile"
	"github.com/koron/duckhouse/internal/accesslog"
	"github.com/koron/duckhouse/internal/authn"
	"github.com/koron/duckhouse/internal/conndb"
	"github.com/koron/duckhouse/internal/duckdbinit"
	"github.com/koron/duckhouse/internal/fileserver"
	"github.com/koron/duckhouse/internal/formatter"
	"github.com/koron/duckhouse/internal/httperror"
	"github.com/koron/duckhouse/internal/querydb"
)

const (
	AuthnIDHeader      = "Duckhouse-Authnid"
	ConnectionIDHeader = "Duckhouse-Connectionid"
	DurationHeader     = "Duckhouse-Duration"

	defaultFormat = "csv"
)

var (
	accessLogger *slog.Logger

	queryDatabase querydb.Database

	withoutAuthz    = false
	globalInitQuery string

	dbSettings    duckdbinit.Settings
	dbSharedDir   string
	dbPrivateRoot string

	uiFS fs.FS
)

var (
	ErrNoQuery = errors.New("no queries")
)

func readQuery(r *http.Request) (string, error) {
	b, err := io.ReadAll(r.Body)
	if err != nil {
		return "", err
	}
	if len(b) > 0 {
		return string(b), nil
	}
	q := r.URL.Query()
	if s := q.Get("q"); s != "" {
		return s, nil
	}
	if s := q.Get("query"); s != "" {
		return s, nil
	}
	return "", ErrNoQuery
}

func parseFormat(r *http.Request) (format string, params map[string]string) {
	q := r.URL.Query()
	format = q.Get("format")
	if format == "" {
		format = q.Get("f")
	}
	parts := strings.Split(format, ",")
	if parts[0] == "" {
		parts[0] = defaultFormat
	}
	// Parse parameters
	params = map[string]string{}
	for _, s := range parts[1:] {
		p := strings.SplitN(s, ":", 2)
		if p[0] == "" {
			continue
		}
		if len(p) == 1 {
			params[p[0]] = ""
			continue
		}
		params[p[0]] = p[1]
	}
	return parts[0], params
}

func writeRows(ctx context.Context, fw formatter.Writer, rows *sql.Rows) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	// Write the header
	columnTypes, err := rows.ColumnTypes()
	if err != nil {
		return err
	}
	err = fw.WriteHeader(columnTypes)
	if err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	// Prepare for scan
	receivers := make([]any, len(columnTypes))
	values := make([]any, len(columnTypes))
	for i := range receivers {
		receivers[i] = new(any)
	}
	for rows.Next() {
		if err := ctx.Err(); err != nil {
			return err
		}
		err := rows.Scan(receivers...)
		if err != nil {
			return err
		}
		for i, pv := range receivers {
			values[i] = *pv.(*any)
		}
		err = fw.WriteBody(values)
		if err != nil {
			return err
		}
	}
	return fw.Flush()
}

func checkAuthz(w http.ResponseWriter, r *http.Request) error {
	if !authn.Enable() {
		return nil
	}
	if id, ok := authn.AuthnID(r.Context()); ok {
		// Insert AuthnID to response header.
		w.Header().Set(AuthnIDHeader, id.String())
		return nil
	}
	if withoutAuthz {
		return nil
	}
	return httperror.New(401)
}

func handleQuery(w http.ResponseWriter, r *http.Request) error {
	if err := checkAuthz(w, r); err != nil {
		return err
	}
	if r.Method != "GET" && r.Method != "POST" {
		return httperror.New(404)
	}
	query, err := readQuery(r)
	if err != nil {
		if r.Method == "GET" && errors.Is(err, ErrNoQuery) {
			http.Redirect(w, r, "/ui/", http.StatusTemporaryRedirect)
			return nil
		}
		return httperror.Newf(400, "No queries: %s", err)
	}

	// determine format from the request
	format, params := parseFormat(r)
	slog.Debug("parsed format", "format", format, "params", params)
	factory, ok := formatter.Find(format)
	if !ok {
		return httperror.Newf(400, "Unsupported format: %s", format)
	}
	fw, err := factory.Create(w, params)
	if err != nil {
		return httperror.Newf(400, "Invalid parameters for the format: %s params=%+v", format, params)
	}

	// Determine database
	db, connid, err := conndb.GetDB(r.Context())
	if connid != 0 {
		w.Header().Set(ConnectionIDHeader, connid.String())
	}
	if err != nil {
		if errors.Is(err, conndb.ErrMaxDB) {
			return httperror.Newf(429, err.Error())
		}
		return httperror.Newf(500, "No associated DB: %s", err)
	}

	// Register an executing query, and defer unregister it.
	q := queryDatabase.Add(r.Context(), connid, query)
	defer q.Close()

	// Execute a query
	rows, err := db.QueryContext(q.Context(), query)
	dur := time.Since(q.Start)
	if r, ok := w.(accesslog.QueryReporter); ok {
		r.QueryReport(query, dur)
	}
	w.Header().Set(DurationHeader, dur.String())
	if err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return httperror.Newf(504, err.Error())
		}
		if _, ok := err.(*duckdb.Error); !ok {
			return httperror.Newf(500, "DB error: %s", err)
		}
		return httperror.Newf(400, "Query error: %s", err)
	}
	defer rows.Close()

	// Write the response body
	w.Header().Set("Content-Type", factory.ContentType())
	w.WriteHeader(200)
	err = writeRows(q.Context(), fw, rows)
	if err != nil {
		return httperror.Newf(500, "Serialization error: %s", err)
	}
	return nil
}

type ConnectionStatus struct {
	ID      string      `json:"ID"`
	DBStats sql.DBStats `json:"DBStats"`
}

func handlePing(w http.ResponseWriter, r *http.Request) error {
	w.WriteHeader(200)
	w.Write([]byte("OK\r\n"))
	return nil
}

func handleStatusConnections(w http.ResponseWriter, r *http.Request) error {
	w.Header().Set("Content-Type", "application/jsonlines")
	w.WriteHeader(200)
	enc := json.NewEncoder(w)
	for id, db := range conndb.Default.Databases() {
		s := ConnectionStatus{
			ID:      id.String(),
			DBStats: db.Stats(),
		}
		if err := enc.Encode(s); err != nil {
			return err
		}
		if _, err := w.Write([]byte("\n")); err != nil {
			return err
		}
	}
	return nil
}

func handleStatusQueries(w http.ResponseWriter, r *http.Request) error {
	w.Header().Set("Content-Type", "application/jsonlines")
	w.WriteHeader(200)
	now := time.Now()
	enc := json.NewEncoder(w)
	for _, q := range queryDatabase.Queries() {
		if err := enc.Encode(q.Stats(now)); err != nil {
			return err
		}
		if _, err := w.Write([]byte("\n")); err != nil {
			return err
		}
	}
	return nil
}

func handleInterruptQuery(w http.ResponseWriter, r *http.Request) error {
	if err := checkAuthz(w, r); err != nil {
		return err
	}
	id, err := querydb.ParseID(r.PathValue("queryID"))
	if err != nil {
		return httperror.Newf(400, "ID syntax error: %s", err)
	}
	q, ok := queryDatabase.Query(id)
	if !ok {
		return httperror.New(404)
	}
	q.Close()
	w.WriteHeader(204)
	return nil
}

func getPrivateDir(ctx context.Context, makeDir bool) (string, error) {
	if dbPrivateRoot == "" {
		return "", nil
	}
	connID, ok := conndb.GetID(ctx)
	if !ok {
		slog.Debug("connection ID cannot be determined")
		return "", nil
	}
	privateDir := filepath.Join(dbPrivateRoot, connID.String())
	if makeDir {
		if err := os.MkdirAll(privateDir, 0777); err != nil {
			return "", err
		}
	}
	return privateDir, nil
}

func newDuckDB(ctx context.Context) (*sql.DB, error) {
	// Compose duckdbinit.Settings
	settings := dbSettings
	if dbSharedDir != "" {
		if err := os.MkdirAll(dbSharedDir, 0777); err != nil {
			return nil, err
		}
		settings.AllowedDirectories = append(settings.AllowedDirectories, dbSharedDir)
	}
	privateDir, err := getPrivateDir(ctx, true)
	if err != nil {
		return nil, err
	}
	if privateDir != "" {
		settings.AllowedDirectories = append(settings.AllowedDirectories, privateDir)
	}
	// Prepare initQueries
	initQueries := make([]string, 0, 4)
	if dbSharedDir != "" {
		initQueries = append(initQueries, fmt.Sprintf("CREATE MACRO public_dir(name) AS concat('%s', '/', name)", dbSharedDir))
	}
	if privateDir != "" {
		initQueries = append(initQueries, fmt.Sprintf("CREATE MACRO private_dir(name) AS concat('%s', '/', name)", privateDir))
	}
	if globalInitQuery != "" {
		initQueries = append(initQueries, globalInitQuery)
	}
	if entry, ok := authn.AuthnEntry(ctx); ok && entry.InitQuery != "" {
		initQueries = append(initQueries, entry.InitQuery)
	}
	return duckdbinit.Open(ctx, settings, initQueries...)
}

func closeDuckDB(ctx context.Context, db *sql.DB) error {
	privateDir, _ := getPrivateDir(ctx, false)
	if privateDir != "" {
		if err := os.RemoveAll(privateDir); err != nil {
			slog.Warn("failed to remove private directory", "dir", privateDir, "error", err)
		}
	}
	return db.Close()
}

func checkDB(ctx context.Context) error {
	db, err := newDuckDB(ctx)
	if err != nil {
		return err
	}
	defer db.Close()
	return db.PingContext(ctx)
}

func errorAwareHandler(handle func(http.ResponseWriter, *http.Request) error) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		err := handle(w, r)
		if err != nil {
			httperror.Write(w, err)
		}
	})
}

func authzChangeOperationHanlder(handle http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case "GET", "HEAD", "OPTIONS", "PROPFIND":
			// no authz
		default:
			// Under authz control
			err := checkAuthz(w, r)
			if err != nil {
				httperror.Write(w, err)
				return
			}
		}
		handle.ServeHTTP(w, r)
	})
}

func newDuckhouseHandler(logger *slog.Logger) http.Handler {
	mux := http.NewServeMux()
	mux.Handle("/{$}", errorAwareHandler(handleQuery))
	mux.Handle("GET /ping/{$}", errorAwareHandler(handlePing))
	mux.Handle("GET /status/connections/{$}", errorAwareHandler(handleStatusConnections))
	mux.Handle("GET /status/queries/{$}", errorAwareHandler(handleStatusQueries))
	mux.Handle("DELETE /status/queries/{queryID}", errorAwareHandler(handleInterruptQuery))
	mux.Handle("/ui/", http.StripPrefix("/ui/", http.FileServerFS(uiFS)))
	if dbSharedDir != "" {
		h := authzChangeOperationHanlder(fileserver.New(dbSharedDir))
		mux.Handle("/shared/", http.StripPrefix("/shared/", h))
	}
	var h http.Handler = mux
	h = accesslog.WrapHandler(logger, h)
	h = authn.WrapHandler(h)
	return h
}

func startServer(ctx context.Context, addr string) error {
	// Preparement: check database configuration.
	err := checkDB(context.Background())
	if err != nil {
		return err
	}

	// Start server
	srv := &http.Server{
		Addr:        addr,
		Handler:     newDuckhouseHandler(accessLogger),
		ConnContext: conndb.ConnContext,
		ConnState:   conndb.ConnState,
	}
	slog.Info("listening on", "addr", srv.Addr)
	return ctxsrv.HTTP(srv).WithShutdownTimeout(time.Minute).ServeWithContext(ctx)
}

func stringOrReadFile(s, purpose string) (string, error) {
	if !strings.HasPrefix(s, "@") {
		return s, nil
	}
	b, err := os.ReadFile(s[1:])
	if err != nil {
		return "", err
	}
	return string(b), nil
}

func getwd() string {
	wd, err := os.Getwd()
	if err != nil {
		return ""
	}
	return wd
}

func run() error {
	var (
		debugFlag bool

		maxDB int
		addr  string

		pidfileName     string
		accessLogFile   string
		accessLogFormat string

		authnFile string
		noauthz   bool

		dbThreads        int
		dbMemoryLimiit   string
		dbHomeDir        string
		dbMaxTempDirSize string
		dbInitQuery      string
		dbExternalAccess bool
		dbLockConfig     bool

		uiResourceDir string
	)

	flag.BoolVar(&debugFlag, "debug", false, `enable debug log`)
	flag.IntVar(&maxDB, "maxdb", 20, `maximum number of DB instances`)
	flag.StringVar(&addr, "addr", "localhost:9998", `address hosts HTTP server`)
	flag.StringVar(&pidfileName, "pidfile", "", `file to record the process ID`)
	flag.StringVar(&accessLogFormat, "accesslog.format", "text", `access log format: "text" or "json"`)
	flag.StringVar(&accessLogFile, "accesslog.file", "", `access log file (default: stdout)`)
	flag.StringVar(&authnFile, "authnfile", "", `authentication information file`)
	flag.BoolVar(&noauthz, "noauthz", false, `executing queries etc. w/o authz`)
	flag.IntVar(&dbThreads, "db.threads", 1, `initial value of DB "threads"`)
	flag.StringVar(&dbMemoryLimiit, "db.memorylimit", "1GiB", `initial value of DB "memory_limit"`)
	flag.StringVar(&dbHomeDir, "db.homedir", filepath.Join(getwd(), ".duckdb"), `home dir for duckdb`)
	flag.StringVar(&dbMaxTempDirSize, "db.maxtempdirsize", "10GiB", `max size of temporary dir`)
	flag.BoolVar(&dbExternalAccess, "db.externalaccess", true, `enable external access. to disable -db.externalaccess=false`)
	flag.BoolVar(&dbLockConfig, "db.lockconfig", true, `lock DB settings. to unlock -db.lockconfig=false`)
	flag.StringVar(&dbInitQuery, "db.initquery", "", `DB initialization query or file (prefixed with '@')`)
	flag.StringVar(&uiResourceDir, "ui.resourcedir", "", `UI resource directory for development`)
	flag.Parse()

	if debugFlag {
		slog.SetLogLoggerLevel(slog.LevelDebug)
	}

	if pidfileName != "" {
		err := pidfile.Write(pidfileName)
		if err != nil {
			return fmt.Errorf("failed to write PID file: %w", err)
		}
		defer pidfile.Close()
	}

	var accessLogWriter io.Writer = os.Stdout
	if accessLogFile != "" {
		w, err := hupfile.New(accessLogFile)
		if err != nil {
			return fmt.Errorf("failed to open access log file: %w", err)
		}
		accessLogWriter = w
	}
	switch accessLogFormat {
	case "text":
		accessLogger = slog.New(slog.NewTextHandler(accessLogWriter, nil))
	case "json":
		accessLogger = slog.New(slog.NewJSONHandler(accessLogWriter, nil))
	default:
		return fmt.Errorf("unsupported access log format: %q", accessLogFormat)
	}

	fs, err := getUIFS(uiResourceDir)
	if err != nil {
		return fmt.Errorf("UI resource failure: %w", err)
	}
	uiFS = fs

	conndb.SetMaxDB(maxDB)
	conndb.SetOpener(conndb.OpenerFunc(newDuckDB))
	conndb.SetCloser(conndb.CloserFunc(closeDuckDB))

	if authnFile != "" {
		err := authn.ReadFile(authnFile)
		if err != nil {
			return fmt.Errorf("authnfile error: %w", err)
		}
	}
	if noauthz {
		if !authn.Enable() {
			return errors.New("-noauthz need to be used with -authnfile")
		}
		withoutAuthz = true
	}

	sharedDir, err := filepath.Abs(filepath.Join(dbHomeDir, "shared"))
	if err != nil {
		return fmt.Errorf("failed to determine shared directory: %w", err)
	}
	dbSharedDir = sharedDir

	privateRoot, err := filepath.Abs(filepath.Join(dbHomeDir, "private"))
	if err != nil {
		return fmt.Errorf("failed to determine private root: %w", err)
	}
	dbPrivateRoot = privateRoot

	dbSettings = duckdbinit.Settings{
		HomeDir:        dbHomeDir,
		Threads:        dbThreads,
		MemoryLimit:    dbMemoryLimiit,
		ExtensionDir:   filepath.Join(dbHomeDir, "extensions"),
		SecretDir:      filepath.Join(dbHomeDir, "stored_secrets"),
		TempDir:        filepath.Join(dbHomeDir, "tmp"),
		MaxTempDirSize: dbMaxTempDirSize,

		EnableExternalAccess: dbExternalAccess,
		LockConfig:           dbLockConfig,
	}
	if dbInitQuery != "" {
		q, err := stringOrReadFile(dbInitQuery, "db.initquery")
		if err != nil {
			return fmt.Errorf("db.initquery failure: %w", err)
		}
		globalInitQuery = q
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	if err := startServer(ctx, addr); err != nil {
		return fmt.Errorf("server terminated: %w", err)
	}

	return nil
}

func main() {
	if err := run(); err != nil {
		slog.Error("duckhouse terminated", "error", err)
		os.Exit(1)
	}
}
