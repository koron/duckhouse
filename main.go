package main

import (
	"context"
	"database/sql"
	"encoding/csv"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"net"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/duckdb/duckdb-go/v2"
	"github.com/koron/duckhouse/internal/httperror"
)

var (
	idSet    sync.Map
	connToID sync.Map
	idToDB   sync.Map

	dbCount int
	dbMutex sync.Mutex

	accessLogWriter io.Writer = os.Stdout

	nullStr = "NULL"
)

type connID uint64

func (id connID) String() string {
	return fmt.Sprintf("%016x", uint64(id))
}

func duckhouseNewConnID(c net.Conn) connID {
	for {
		id := connID(rand.Uint64())
		_, ok := idSet.LoadOrStore(id, true)
		if !ok {
			connToID.Store(c, id)
			return id
		}
	}
}

func dbToLogArg(db *sql.DB) string {
	return fmt.Sprintf("%p", db)
}

var errMaxDB = errors.New("reached maximum number of DB")

func duckhouseGetDB(r *http.Request) (*sql.DB, connID, error) {
	id, ok := r.Context().Value(connIDKey{}).(connID)
	if !ok {
		return nil, 0, fmt.Errorf("no connection ID assigned for request:%v", r)
	}
	rawdb, ok := idToDB.Load(id)
	if ok {
		return rawdb.(*sql.DB), id, nil
	}
	// Create a new database for the connection
	dbMutex.Lock()
	defer dbMutex.Unlock()
	if dbCount >= maxDB {
		return nil, 0, errMaxDB
	}
	db, err := sql.Open("duckdb", "")
	if err != nil {
		return nil, 0, err
	}
	db.SetMaxIdleConns(0)
	idToDB.Store(id, db)
	dbCount++
	slog.Debug("DB opened", "connID", id, "DB", dbToLogArg(db), "count", dbCount)
	return db, id, nil
}

type connIDKey = struct{}

func duckhouseConnContext(ctx context.Context, c net.Conn) context.Context {
	id := duckhouseNewConnID(c)
	return context.WithValue(ctx, connIDKey{}, id)
}

func duckhouseCloseConn(c net.Conn) error {
	rawid, ok := connToID.LoadAndDelete(c)
	if !ok {
		return fmt.Errorf("no ID for net.Conn=%p", c)
	}
	dbMutex.Lock()
	defer dbMutex.Unlock()
	id := rawid.(connID)
	idSet.Delete(id)
	rawdb, ok := idToDB.LoadAndDelete(id)
	if !ok {
		return nil
	}
	db := rawdb.(*sql.DB)
	db.Close()
	if dbCount > 0 {
		dbCount--
	}
	slog.Debug("DB closed", "connID", id, "DB", dbToLogArg(db), "count", dbCount)
	return nil
}

func duckhouseConnState(c net.Conn, s http.ConnState) {
	if s == http.StateClosed {
		err := duckhouseCloseConn(c)
		if err != nil {
			slog.Warn("failed to close DB", "error", err)
		}
	}
}

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

func anyToStr(v any) string {
	return fmt.Sprint(v)
}

func blobToStr(v any) string {
	return string(v.([]uint8))
}

func writeAsCSV(w http.ResponseWriter, rows *sql.Rows) error {
	w.Header().Set("Content-Type", "text/csv")
	w.WriteHeader(200)

	ww := csv.NewWriter(w)

	for {
		// Write header
		types, err := rows.ColumnTypes()
		if err != nil {
			return err
		}
		// Choose the right stringification function depending on the type name
		// in your database
		strfuncs := make([]func(any) string, len(types))
		records := make([]string, len(types))
		for i, typ := range types {
			records[i] = typ.Name()
			switch typ.DatabaseTypeName() {
			case "BLOB":
				strfuncs[i] = blobToStr
			default:
				strfuncs[i] = anyToStr
			}
			// FIXME: support other special types
		}
		// Write the header
		if len(types) > 0 {
			err := ww.Write(records)
			if err != nil {
				return err
			}
		}
		// Scan and write values (CSV body)
		values := make([]any, len(types))
		for i := range values {
			values[i] = new(any)
		}
		for rows.Next() {
			err := rows.Scan(values...)
			if err != nil {
				return err
			}
			for i, pv := range values {
				v := *pv.(*any)
				if v == nil {
					records[i] = nullStr
					continue
				}
				records[i] = strfuncs[i](v)
			}
			if err := ww.Write(records); err != nil {
				return err
			}
		}
		if !rows.NextResultSet() {
			break
		}
	}

	ww.Flush()
	return nil
}

func duckhouseHandleQuery(w http.ResponseWriter, r *http.Request) error {
	if r.Method != "GET" && r.Method != "POST" {
		return httperror.New(404)
	}
	query, err := readQuery(r)
	if err != nil {
		return httperror.Newf(400, "No queries: %s", err)
	}

	db, id, err := duckhouseGetDB(r)
	if err != nil {
		if err == errMaxDB {
			return httperror.Newf(429, err.Error())
		}
		return httperror.Newf(500, "No associated DB: %s", err)
	}
	w.Header().Set("Duckhouse-Connectionid", id.String())
	slog.Debug("queried", "connID", id, "query", query)
	rows, err := db.QueryContext(r.Context(), query)
	if err != nil {
		if _, ok := err.(*duckdb.Error); !ok {
			return httperror.Newf(500, "DB error: %s", err)
		}
		return httperror.Newf(400, "Query error: %s", err)
	}
	defer rows.Close()

	err = writeAsCSV(w, rows)
	if err != nil {
		return httperror.Newf(500, "Serialization error: %s", err)
	}
	return nil
}

func duckhouseHandler(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path == "/" {
		err := duckhouseHandleQuery(w, r)
		if err != nil {
			httperror.Write(w, err)
		}
		return
	}
	if r.URL.Path == "/ping" || strings.HasPrefix(r.URL.Path, "/ping/") {
		w.WriteHeader(200)
		w.Write([]byte("OK\r\n"))
		return
	}
	httperror.Write(w, httperror.New(404))
}

type wrapResponseWriter struct {
	base   http.ResponseWriter
	status int
	bsize  int
}

func (w *wrapResponseWriter) Header() http.Header {
	return w.base.Header()
}

func (w *wrapResponseWriter) Write(data []byte) (int, error) {
	n, err := w.base.Write(data)
	w.bsize += n
	return n, err
}

func (w *wrapResponseWriter) WriteHeader(statusCode int) {
	w.base.WriteHeader(statusCode)
	w.status = statusCode
}

func recordCombinedAccessLog(w io.Writer, r *http.Request, status, bodySize int) {
	remoteAddr := r.RemoteAddr
	ident := "-"
	user := "-"
	if r.URL.User != nil {
		user = r.URL.User.Username()
	}
	timestamp := time.Now().Format("02/Jan/2006:15:04:05 -0700")
	requestLine := fmt.Sprintf("%s %s %s", r.Method, r.URL.RequestURI(), r.Proto)
	referer := r.Referer()
	if referer == "" {
		referer = "-"
	}
	userAgent := r.UserAgent()
	if userAgent == "" {
		userAgent = "-"
	}
	fmt.Fprintf(w, "%s %s %s [%s] \"%s\" %d %d \"%s\" \"%s\"\n",
		remoteAddr, ident, user, timestamp, requestLine, status, bodySize, referer, userAgent)
}

func middlewareCombinedAccessLogger(logWriter io.Writer, h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ww := &wrapResponseWriter{base: w}
		h.ServeHTTP(ww, r)
		recordCombinedAccessLog(logWriter, r, ww.status, ww.bsize)
	})
}

func run() error {
	srv := &http.Server{
		Addr:        "localhost:9998",
		Handler:     middlewareCombinedAccessLogger(accessLogWriter, http.HandlerFunc(duckhouseHandler)),
		ConnContext: duckhouseConnContext,
		ConnState:   duckhouseConnState,
	}
	slog.Info("listening on", "addr", srv.Addr)
	return srv.ListenAndServe()
}

var (
	debugFlag bool
	maxDB     int
)

func main() {
	flag.BoolVar(&debugFlag, "debug", false, `enable debug log`)
	flag.IntVar(&maxDB, "maxdb", 4, `maximum number of DB instances`)
	flag.Parse()
	if debugFlag {
		slog.SetLogLoggerLevel(slog.LevelDebug)
	}
	if err := run(); err != nil {
		slog.Error("server terminated", "error", err)
	}
}
