package formattertest

import (
	"bytes"
	"database/sql"
	"net/url"
	"testing"

	_ "github.com/duckdb/duckdb-go/v2"
	"github.com/koron/duckhouse/internal/formatter"
)

func Find[T formatter.Factory](t *testing.T, name string) T {
	t.Helper()
	f, ok := formatter.Find(name)
	if !ok {
		t.Fatalf("not find formatter: %s", name)
	}
	v, ok := f.(T)
	if !ok {
		var want T
		t.Fatalf("unexpected factory: want=%T got=%T", want, f)
	}
	return v
}

func WriteRows(formatWriter formatter.Writer, rows *sql.Rows) error {
	// Write the header
	columnTypes, err := rows.ColumnTypes()
	if err != nil {
		return err
	}
	err = formatWriter.WriteHeader(columnTypes)
	if err != nil {
		return err
	}
	// Prepare for scan
	receivers := make([]any, len(columnTypes))
	values := make([]any, len(columnTypes))
	for i := range receivers {
		receivers[i] = new(any)
	}
	for rows.Next() {
		err := rows.Scan(receivers...)
		if err != nil {
			return err
		}
		for i, pv := range receivers {
			values[i] = *pv.(*any)
		}
		err = formatWriter.WriteBody(values)
		if err != nil {
			return err
		}
	}
	return formatWriter.Flush()
}

func ConnectDB(t *testing.T) *sql.Conn {
	t.Helper()

	// Open database
	params := url.Values{}
	params.Add("home_directory", t.TempDir())
	db, err := sql.Open("duckdb", "?"+params.Encode())
	if err != nil {
		t.Fatalf("failed to open DuckDB: %s", err)
	}
	t.Cleanup(func() {
		err := db.Close()
		if err != nil {
			t.Helper()
			t.Errorf("failed to close DuckDB: %s", err)
		}
	})

	// Connect to database
	c, err := db.Conn(t.Context())
	if err != nil {
		t.Fatalf("failed to connect DuckDB: %s", err)
	}
	t.Cleanup(func() {
		c.Close()
		if err != nil {
			t.Helper()
			t.Errorf("failed to close DuckDB connection: %s", err)
		}
	})

	return c
}

func Query(t *testing.T, conn *sql.Conn, format string, query string, args ...any) *bytes.Buffer {
	t.Helper()
	rows, err := conn.QueryContext(t.Context(), query, args...)
	if err != nil {
		t.Fatalf("failed to query: %s", err)
	}
	bb := &bytes.Buffer{}
	_, writer, err := formatter.FindAndCreate(format, bb)
	if err != nil {
		t.Fatal(err)
	}
	if err := WriteRows(writer, rows); err != nil {
		t.Fatalf("failed to write rows: %s", err)
	}
	return bb
}
