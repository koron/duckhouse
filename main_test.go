package main

import (
	"bufio"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/koron/duckhouse/internal/conndb"
	"github.com/koron/duckhouse/internal/duckdbinit"
)

func assertEqual[T any](t *testing.T, want, got T, options ...cmp.Option) {
	t.Helper()
	if d := cmp.Diff(want, got, options...); d != "" {
		t.Errorf("assert failed, mismatch: -want +got\n%s", d)
	}
}

func startServer0(t *testing.T) *httptest.Server {
	var (
		dbHomeDir        = t.TempDir()
		maxDB            = 4
		dbThreads        = 1
		dbMemoryLimiit   = "1GiB"
		dbMaxTempDirSize = "4GiB"
		dbLockConfig     = true
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

func readJSONL[T any](r *http.Response, err error) ([]T, error) {
	if err != nil {
		return nil, fmt.Errorf("http failed: %w", err)
	}
	defer r.Body.Close()
	if r.StatusCode < 200 || r.StatusCode > 299 {
		return nil, fmt.Errorf("request failed: %d (%s)", r.StatusCode, r.Status)
	}
	var list []T
	scanner := bufio.NewScanner(r.Body)
	for scanner.Scan() {
		var v T
		b := scanner.Bytes()
		if len(b) == 0 {
			continue
		}
		err := json.Unmarshal(b, &v)
		if err != nil {
			return nil, fmt.Errorf("unmarshal failed: %w", err)
		}
		list = append(list, v)
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("scanner failed: %w", err)
	}
	return list, nil
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

type TestConnStatus struct {
	ID      string      `json:"ID"`
	DBStats sql.DBStats `json:"DBStats"`
}

func TestStatusConnections(t *testing.T) {
	ts := startServer0(t)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		testQuery0(t, ts, `SELECT version() AS V`, "V\nv1.5.0\n")
		time.Sleep(200 * time.Millisecond)
		testQuery0(t, ts, `SELECT version() AS V`, "V\nv1.5.0\n")
		wg.Done()
	}()
	time.Sleep(100 * time.Millisecond)
	got, err := readJSONL[TestConnStatus](doGet(ts, "/status/connections/"))
	if err != nil {
		t.Error(err)
		return
	}
	assertEqual(t, []TestConnStatus{
		{DBStats: sql.DBStats{MaxIdleClosed: 2}},
	}, got, cmpopts.IgnoreFields(TestConnStatus{}, "ID"))
	wg.Wait()
}
