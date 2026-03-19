package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"flag"
	"io"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/duckdb/duckdb-go/v2"
	"github.com/koron/duckhouse/internal/authn"
	"github.com/koron/duckhouse/internal/accesslog"
	"github.com/koron/duckhouse/internal/conndb"
	"github.com/koron/duckhouse/internal/duckdbinit"
	"github.com/koron/duckhouse/internal/formatter"
	"github.com/koron/duckhouse/internal/httperror"
	"github.com/koron/duckhouse/internal/querydb"
)

const (
	AuthnIDHeader      = "Duckhouse-Authnid"
	ConnectionIDHeader = "Duckhouse-Connectionid"
	DurationHeader     = "Duckhouse-Duration"
)

var (
	accessLogger *slog.Logger

	defaultFormat = "csv"
	queryDatabase querydb.Database

	withoutAuthz = false
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
	return "", errors.New("no queries")
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
	if id, ok := authn.AuthnID(r); ok {
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

func newDuckDB(ctx context.Context) (*sql.DB, error) {
	return duckdbinit.Open(ctx)
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

func newDuckhouseHandler(logger *slog.Logger) http.Handler {
	mux := http.NewServeMux()
	mux.Handle("/{$}", errorAwareHandler(handleQuery))
	mux.Handle("GET /ping/{$}", errorAwareHandler(handlePing))
	mux.Handle("GET /status/connections/{$}", errorAwareHandler(handleStatusConnections))
	mux.Handle("GET /status/queries/{$}", errorAwareHandler(handleStatusQueries))
	mux.Handle("DELETE /status/queries/{queryID}", errorAwareHandler(handleInterruptQuery))
	var h http.Handler = mux
	h = accesslog.WrapHandler(logger, h)
	h = authn.WrapHandler(h)
	return h
}

func run(addr string) error {
	err := checkDB(context.Background())
	if err != nil {
		return err
	}
	srv := &http.Server{
		Addr:        addr,
		Handler:     newDuckhouseHandler(accessLogger),
		ConnContext: conndb.ConnContext,
		ConnState:   conndb.ConnState,
	}
	slog.Info("listening on", "addr", srv.Addr)
	return srv.ListenAndServe()
}

func stringOrReadFile(s, purpose string) (string, error) {
	if !strings.HasPrefix(s, "@") {
		return s, nil
	}
	b, err := os.ReadFile(s[1:])
	if err != nil {
		return "", err
	}
	slog.Debug("read file", "purpose", purpose, "name", s[1:])
	return string(b), nil
}

func getwd() string {
	wd, err := os.Getwd()
	if err != nil {
		return ""
	}
	return wd
}

func main() {
	var (
		debugFlag bool

		maxDB int
		addr  string

		accessLogFormat string

		authnFile string
		noauthz   bool

		dbThreads        int
		dbMemoryLimiit   string
		dbHomeDir        string
		dbMaxTempDirSize string
		dbInitQuery      string
		dbLockConfig     bool
	)

	flag.BoolVar(&debugFlag, "debug", false, `enable debug log`)
	flag.IntVar(&maxDB, "maxdb", 4, `maximum number of DB instances`)
	flag.StringVar(&addr, "addr", "localhost:9998", `address hosts HTTP server`)
	flag.StringVar(&accessLogFormat, "accesslog.format", "text", `access log format: "text" or "json"`)
	flag.StringVar(&authnFile, "authnfile", "", `authentication information file`)
	flag.BoolVar(&noauthz, "noauthz", false, `executing queries etc. w/o authz`)
	flag.IntVar(&dbThreads, "db.threads", 1, `initial value of DB "threads"`)
	flag.StringVar(&dbMemoryLimiit, "db.memorylimit", "1GiB", `initial value of DB "memory_limit"`)
	flag.StringVar(&dbHomeDir, "db.homedir", filepath.Join(getwd(), ".duckdb"), `home dir for duckdb`)
	flag.StringVar(&dbMaxTempDirSize, "db.maxtempdirsize", "10GiB", `max size of temporary dir`)
	flag.BoolVar(&dbLockConfig, "db.lockconfig", true, `lock DB settings. to unlock use -db.lockconfig=false`)
	flag.StringVar(&dbInitQuery, "db.initquery", "", `DB initialization query or file (prefixed with '@')`)
	flag.Parse()

	if debugFlag {
		slog.SetLogLoggerLevel(slog.LevelDebug)
	}

	switch accessLogFormat {
	case "text":
		accessLogger = slog.New(slog.NewTextHandler(os.Stdout, nil))
	case "json":
		accessLogger = slog.New(slog.NewJSONHandler(os.Stdout, nil))
	default:
		slog.Error("unsupported access log format", "format", accessLogFormat)
		os.Exit(1)
	}

	conndb.SetMaxDB(maxDB)
	conndb.SetOpener(conndb.OpenerFunc(newDuckDB))

	if authnFile != "" {
		err := authn.ReadFile(authnFile)
		if err != nil {
			slog.Error("authnfile failure", "error", err)
			os.Exit(1)
		}
	}
	if noauthz {
		if !authn.Enable() {
			slog.Error("-noauthz needs to be used with -authnfile.")
			os.Exit(1)
		}
		withoutAuthz = true
	}

	duckdbinit.DefaultSettings = duckdbinit.Settings{
		HomeDir:        dbHomeDir,
		Threads:        dbThreads,
		MemoryLimit:    dbMemoryLimiit,
		ExtensionDir:   filepath.Join(dbHomeDir, "extensions"),
		SecretDir:      filepath.Join(dbHomeDir, "stored_secrets"),
		TempDir:        filepath.Join(dbHomeDir, "tmp"),
		MaxTempDirSize: dbMaxTempDirSize,
		LockConfig:     dbLockConfig,
	}
	if dbInitQuery != "" {
		q, err := stringOrReadFile(dbInitQuery, "db.initquery")
		if err != nil {
			slog.Error("db.initquery failuare", "error", err)
			os.Exit(1)
		}
		duckdbinit.InitQuery = q
	}

	if err := run(addr); err != nil {
		slog.Error("server terminated", "error", err)
		os.Exit(1)
	}
}
