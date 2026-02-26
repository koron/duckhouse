package main

import (
	"context"
	"database/sql"
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"math/rand/v2"
	"net"
	"net/http"
	"strings"
	"sync"

	_ "github.com/duckdb/duckdb-go/v2"
)

var (
	idSet    sync.Map
	connToID sync.Map
	idToDB   sync.Map
)

func duckhouseNewConnID(c net.Conn) uint64 {
	for {
		id := rand.Uint64()
		_, ok := idSet.LoadOrStore(id, true)
		if !ok {
			connToID.Store(c, id)
			return id
		}
	}
}

func duckhouseGetDB(r *http.Request) (*sql.DB, uint64, error) {
	id, ok := r.Context().Value(connIDKey{}).(uint64)
	if !ok {
		return nil, 0, fmt.Errorf("no connection ID assigned for request:%v", r)
	}
	rawdb, ok := idToDB.Load(id)
	if ok {
		return rawdb.(*sql.DB), 0, nil
	}
	db, err := sql.Open("duckdb", "")
	if err != nil {
		return nil, 0, err
	}
	db.SetMaxIdleConns(0)
	idToDB.Store(id, db)
	log.Printf("created sql.DB=%p for connID=%016x", db, id)
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
	id := rawid.(uint64)
	idSet.Delete(id)
	rawdb, ok := idToDB.LoadAndDelete(id)
	if !ok {
		return nil
	}
	db := rawdb.(*sql.DB)
	db.Close()
	log.Printf("closed sql.DB=%p for connID=%016x", db, id)
	return nil
}

func duckhouseConnState(c net.Conn, s http.ConnState) {
	if s == http.StateClosed {
		err := duckhouseCloseConn(c)
		if err != nil {
			log.Printf("failed to close conn: %s", err)
		}
	}
}

func readQuery(r *http.Request) string {
	b, err := io.ReadAll(r.Body)
	if err != nil {
		log.Printf("failed to read request body: %s", err)
	}
	if len(b) > 0 {
		return string(b)
	}
	q := r.URL.Query()
	if s := q.Get("q"); s != "" {
		return s
	}
	if s := q.Get("query"); s != "" {
		return s
	}
	return ""
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
		if len(types) > 0 {
			names := make([]string, len(types))
			fmt.Println("Column types:")
			for i, typ := range types {
				names[i] = typ.Name()
				fmt.Printf("  #%d: name=%s %s\n", i, typ.Name(), typ.ScanType())
			}
			if err := ww.Write(names); err != nil {
				return err
			}
		}
		// Scan and write values (CSV body)
		values := make([]any, len(types))
		records := make([]string, len(types))
		for i := range values {
			values[i] = new(any)
		}
		for rows.Next() {
			err := rows.Scan(values...)
			if err != nil {
				return err
			}
			for i, v := range values {
				records[i] = fmt.Sprint(*(v.(*any)))
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

func duckhouseHandleQuery(w http.ResponseWriter, r *http.Request) {
	if r.Method != "GET" && r.Method != "POST" {
		w.WriteHeader(404)
		io.WriteString(w, "Not Found\r\n")
		return
	}
	query := readQuery(r)
	if query == "" {
		w.WriteHeader(400)
		io.WriteString(w, "No queries, please specify a query\r\n")
		return
	}

	db, id, err := duckhouseGetDB(r)
	if err != nil {
		w.WriteHeader(500)
		io.WriteString(w, "No associated DB: "+err.Error())
		return
	}
	log.Printf("query=%q connID=%016x", query, id)
	rows, err := db.QueryContext(r.Context(), query)
	if err != nil {
		w.WriteHeader(500)
		fmt.Fprintf(w, "Query failed: %s\r\n", err)
		return
	}
	defer rows.Close()

	err = writeAsCSV(w, rows)
	if err != nil {
		w.WriteHeader(500)
		fmt.Fprintf(w, "Serialization error: %s\r\n", err)
	}
}

func duckhouseHandler(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path == "/" {
		duckhouseHandleQuery(w, r)
		return
	}
	if r.URL.Path == "/ping" || strings.HasPrefix(r.URL.Path, "/ping/") {
		w.WriteHeader(200)
		io.WriteString(w, "OK\r\n")
		return
	}
	w.WriteHeader(404)
	io.WriteString(w, "Not Found\r\n")
}

func run2() error {
	srv := &http.Server{
		Addr:        "localhost:9998",
		Handler:     http.HandlerFunc(duckhouseHandler),
		ConnContext: duckhouseConnContext,
		ConnState:   duckhouseConnState,
	}
	log.Printf("listening on %s", srv.Addr)
	return srv.ListenAndServe()
}

func main() {
	if err := run2(); err != nil {
		log.Fatal(err)
	}
}
