package main

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/koron/duckhouse/internal/conndb"
	"github.com/koron/duckhouse/internal/duckdbinit"
)

func assertEqual[T any](t *testing.T, want, got T) {
	t.Helper()
	if d := cmp.Diff(want, got); d != "" {
		t.Errorf("assert failed, mismatch: -want +got\n%s", d)
	}
}

func startServer0(t *testing.T) *httptest.Server {
	var (
		dbHomeDir        string = t.TempDir()
		maxDB            int    = 4
		dbThreads        int    = 1
		dbMemoryLimiit   string = "1GiB"
		dbMaxTempDirSize string = "4GiB"
		dbLockConfig     bool   = true
	)

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

	ts := httptest.NewServer(newDuckhouseHandler(io.Discard))
	t.Cleanup(ts.Close)

	conndb.SetMaxDB(maxDB)
	conndb.SetOpener(conndb.OpenerFunc(newDuckDB))
	ts.Config.ConnContext = conndb.ConnContext
	ts.Config.ConnState = conndb.ConnState

	return ts
}

func doGet(ts *httptest.Server, path string) (*http.Response, error) {
	return ts.Client().Get(ts.URL + path)
}

func doPost(ts *httptest.Server, path, body string) (*http.Response, error) {
	return ts.Client().Post(ts.URL+path, "", strings.NewReader(body))
}

func readResponse(r *http.Response, err error) (string, error) {
	if err != nil {
		return "", fmt.Errorf("http failed: %w", err)
	}
	defer r.Body.Close()
	if r.StatusCode < 200 || r.StatusCode > 299 {
		return "", fmt.Errorf("request failed: %d (%s)", r.StatusCode, r.Status)
	}
	b, err := io.ReadAll(r.Body)
	if err != nil {
		return "", fmt.Errorf("read body failed: %w", err)
	}
	return string(b), nil
}

// testQuery0 checks CSV the response for the query.
func testQuery0(t *testing.T, ts *httptest.Server, query, want string) {
	t.Helper()
	got, err := readResponse(doPost(ts, "/?f=csv", query))
	if err != nil {
		t.Error(err)
		return
	}
	assertEqual(t, want, got)
}

//////////////////////////////////////////////////////////////////////////////
// Test cases

func TestPing(t *testing.T) {
	ts := startServer0(t)
	got, err := readResponse(doGet(ts, "/ping/"))
	if err != nil {
		t.Error(err)
		return
	}
	assertEqual(t, "OK\r\n", got)
}

func TestQueryDuckDBVersion(t *testing.T) {
	ts := startServer0(t)
	testQuery0(t, ts, `SELECT version() AS V`, "V\nv1.5.0\n")
}
