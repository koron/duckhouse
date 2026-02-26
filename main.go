package main

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"strings"
	"sync"

	_ "github.com/duckdb/duckdb-go/v2"
)

func run() error {
	db, err := sql.Open("duckdb", "")
	if err != nil {
		return err
	}
	defer db.Close()

	rows, err := db.Query(`SELECT 'hello', version(), 123 as NUM`)
	if err != nil {
		return err
	}
	defer rows.Close()

	for {
		types, err := rows.ColumnTypes()
		if err != nil {
			return err
		}
		if len(types) > 0 {
			fmt.Println("Column types:")
			for i, typ := range types {
				fmt.Printf("  #%d: name=%s %s\n", i, typ.Name(), typ.ScanType())
			}
		}
		colNum := len(types)
		values := make([]any, colNum)
		for i := range colNum {
			values[i] = new(any)
		}
		rowNum := 0
		fmt.Println("Rows:")
		for rows.Next() {
			err := rows.Scan(values...)
			if err != nil {
				return err
			}
			fmt.Printf("  #%d", rowNum)
			for _, v := range values {
				fmt.Printf(" %+v", *(v.(*any)))
			}
			fmt.Println()
			rowNum++
		}
		if !rows.NextResultSet() {
			break
		}
	}
	return rows.Err()
}

var connToDB sync.Map

func duckhouseBindNewDuckDB(c net.Conn) *sql.DB {
	db, err := sql.Open("duckdb", "")
	if err != nil {
		log.Printf("failed to open DuckDB: %s", err)
		return nil
	}
	db.SetMaxIdleConns(0)
	connToDB.Store(c, db)
	log.Printf("created sql.DB=%p for net.Conn=%p", db, c)
	return db
}

var dbKey = struct{}{}

func duckhouseConnContext(ctx context.Context, c net.Conn) context.Context {
	db := duckhouseBindNewDuckDB(c)
	return context.WithValue(ctx, dbKey, db)
}

func duckhouseConnState(c net.Conn, s http.ConnState) {
	if s == http.StateClosed {
		v, ok := connToDB.LoadAndDelete(c)
		if !ok {
			log.Printf("no sql.DB for net.Conn=%p", c)
			return
		}
		db := v.(*sql.DB)
		db.Close()
		log.Printf("closed sql.DB=%p for net.Conn=%p", db, c)
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

func duckhouseHandleQuery(w http.ResponseWriter, r *http.Request) {
	if r.Method != "GET" && r.Method != "POST" {
		w.WriteHeader(404)
		io.WriteString(w, "Not Found\r\n")
		return
	}
	db, ok := r.Context().Value(dbKey).(*sql.DB)
	if !ok {
		log.Printf("no DuckDB associated to request:%s", r)
		w.WriteHeader(500)
		io.WriteString(w, "No associated database, please contact admin\r\n")
		return
	}
	query := readQuery(r)
	if query == "" {
		w.WriteHeader(400)
		io.WriteString(w, "No queries, please specify a query\r\n")
		return
	}

	rows, err := db.Query(query)
	if err != nil {
		w.WriteHeader(500)
		fmt.Fprintf(w, "Query failed: %s\r\n", err)
		return
	}
	defer rows.Close()

	// TODO:
	log.Printf("query=%s", query)
	_ = db
	w.WriteHeader(501)
	io.WriteString(w, "Not Implemented Yet\r\n")
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
