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
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/duckdb/duckdb-go/v2"
	"github.com/koron/duckhouse/internal/authn"
	"github.com/koron/duckhouse/internal/combinedlog"
	"github.com/koron/duckhouse/internal/conndb"
	"github.com/koron/duckhouse/internal/duckdbinit"
	"github.com/koron/duckhouse/internal/httperror"
)

var (
	accessLogWriter io.Writer = os.Stdout

	nullStr = "NULL"
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

	// Execute a query
	db, id, err := conndb.GetDB(r.Context())
	if id != 0 {
		w.Header().Set("Duckhouse-Connectionid", id.String())
	}
	if err != nil {
		if errors.Is(err, conndb.ErrMaxDB) {
			return httperror.Newf(429, err.Error())
		}
		return httperror.Newf(500, "No associated DB: %s", err)
	}
	start := time.Now()
	rows, err := db.QueryContext(r.Context(), query)
	dur := time.Since(start)
	if r, ok := w.(combinedlog.QueryReporter); ok {
		r.QueryReport(query, dur)
	}
	w.Header().Set("Duckhouse-Duration", dur.String())
	if err != nil {
		if _, ok := err.(*duckdb.Error); !ok {
			return httperror.Newf(500, "DB error: %s", err)
		}
		return httperror.Newf(400, "Query error: %s", err)
	}
	defer rows.Close()

	// Write the response body
	err = writeAsCSV(w, rows)
	if err != nil {
		return httperror.Newf(500, "Serialization error: %s", err)
	}
	return nil
}

func matchPath(r *http.Request, path string) bool {
	if r.URL.Path == path {
		return true
	}
	if len(path) > 0 && path[len(path)-1] != '/' && strings.HasPrefix(r.URL.Path, path+"/") {
		return true
	}
	return false
}

func duckhouseHandler(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path == "/" {
		err := duckhouseHandleQuery(w, r)
		if err != nil {
			httperror.Write(w, err)
		}
		return
	}
	if matchPath(r, "/ping") {
		w.WriteHeader(200)
		w.Write([]byte("OK\r\n"))
		return
	}
	httperror.Write(w, httperror.New(404))
}

func run(addr string) error {
	var h http.Handler = http.HandlerFunc(duckhouseHandler)
	h = combinedlog.WrapHandler(accessLogWriter, h)
	h = authn.WrapHandler(h)
	srv := &http.Server{
		Addr:        addr,
		Handler:     h,
		ConnContext: conndb.ConnContext,
		ConnState:   conndb.ConnState,
	}
	slog.Info("listening on", "addr", srv.Addr)
	return srv.ListenAndServe()
}

func newDuckDB(ctx context.Context) (*sql.DB, error) {
	db, err := sql.Open("duckdb", "")
	if err != nil {
		return nil, err
	}
	if err := duckdbinit.Init(ctx, db); err != nil {
		db.Close()
		return nil, err
	}
	return db, nil
}

func main() {
	var (
		debugFlag bool
		maxDB     int
		addr      string

		dbThreads      int
		dbMemoryLimiit string
	)

	flag.BoolVar(&debugFlag, "debug", false, `enable debug log`)
	flag.IntVar(&maxDB, "maxdb", 4, `maximum number of DB instances`)
	flag.StringVar(&addr, "addr", "localhost:9998", `address hosts HTTP server`)
	flag.IntVar(&dbThreads, "db.threads", 1, `initial value of DB "threads"`)
	flag.StringVar(&dbMemoryLimiit, "db.memorylimit", "1GiB", `initial value of DB "memory_limit"`)
	flag.Parse()
	if debugFlag {
		slog.SetLogLoggerLevel(slog.LevelDebug)
	}
	conndb.SetMaxDB(maxDB)
	conndb.SetOpener(conndb.OpenerFunc(newDuckDB))
	if dbThreads > 0 {
		duckdbinit.DefaultSettings.Threads = &dbThreads
	}
	if dbMemoryLimiit != "" {
		duckdbinit.DefaultSettings.MemoryLimit = &dbMemoryLimiit
	}
	if err := run(addr); err != nil {
		slog.Error("server terminated", "error", err)
	}
}
