package main

import (
	"bufio"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/koron/duckhouse/internal/authn"
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

	dbSettings = duckdbinit.Settings{
		HomeDir:        dbHomeDir,
		Threads:        dbThreads,
		MemoryLimit:    dbMemoryLimiit,
		ExtensionDir:   filepath.Join(dbHomeDir, "extensions"),
		SecretDir:      filepath.Join(dbHomeDir, "stored_secrets"),
		TempDir:        filepath.Join(dbHomeDir, "tmp"),
		MaxTempDirSize: dbMaxTempDirSize,
		LockConfig:     dbLockConfig,
	}
	dbSharedDir = filepath.Join(dbHomeDir, "shared")
	dbPrivateRoot = filepath.Join(dbHomeDir, "private")

	ts := httptest.NewServer(newDuckhouseHandler(slog.New(slog.NewTextHandler(io.Discard, nil))))
	t.Cleanup(ts.Close)

	conndb.SetMaxDB(maxDB)
	conndb.SetOpener(conndb.OpenerFunc(newDuckDB))
	conndb.SetCloser(conndb.CloserFunc(closeDuckDB))

	ts.Config.ConnContext = conndb.ConnContext
	ts.Config.ConnState = conndb.ConnState

	return ts
}

func setupAuthn(t *testing.T, name string, noauthz bool) {
	t.Helper()
	err := authn.ReadFile(name)
	if err != nil {
		t.Fatalf("failed to read authn json file: %s", err)
	}
	withoutAuthz = noauthz
	t.Cleanup(func() {
		authn.Default = nil
		withoutAuthz = false
	})
}

type RequestOption func(*http.Request) *http.Request

func authorizationHeader(value string) RequestOption {
	return func(req *http.Request) *http.Request {
		req.Header.Set("Authorization", value)
		return req
	}
}

func authorizationBasic(name, password string) RequestOption {
	s := base64.StdEncoding.EncodeToString([]byte(name + ":" + password))
	return authorizationHeader("Basic " + s)
}

func authorizationBearer(token string) RequestOption {
	return authorizationHeader("Bearer " + token)
}

func doReq(ts *httptest.Server, req *http.Request, options ...RequestOption) (*http.Response, error) {
	// apply options
	for _, o := range options {
		req = o(req)
	}
	return ts.Client().Do(req)
}

func doGet(ts *httptest.Server, path string, options ...RequestOption) (*http.Response, error) {
	req, err := http.NewRequest("GET", ts.URL+path, nil)
	if err != nil {
		return nil, err
	}
	return doReq(ts, req, options...)
}

func doPost(ts *httptest.Server, path, body string, options ...RequestOption) (*http.Response, error) {
	req, err := http.NewRequest("POST", ts.URL+path, strings.NewReader(body))
	if err != nil {
		return nil, err
	}
	return doReq(ts, req, options...)
}

func doDelete(ts *httptest.Server, path string, options ...RequestOption) (*http.Response, error) {
	req, err := http.NewRequest("DELETE", ts.URL+path, nil)
	if err != nil {
		return nil, err
	}
	return doReq(ts, req, options...)
}

func readResponse(r *http.Response, err error) (string, error) {
	return readResponse2(r, err, 200, 299)
}

func readResponse2(r *http.Response, err error, codeBegin, codeEnd int) (string, error) {
	if err != nil {
		return "", fmt.Errorf("http failed: %w", err)
	}
	defer r.Body.Close()
	if r.StatusCode < codeBegin || r.StatusCode > codeEnd {
		return "", fmt.Errorf("request failed: %d (%s) - should be between %d and %d", r.StatusCode, r.Status, codeBegin, codeEnd)
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
	testQuery1(t, ts, query, want)
}

// testQuery1 checks CSV the response for the query, with RequestOptions.
func testQuery1(t *testing.T, ts *httptest.Server, query, want string, options ...RequestOption) {
	t.Helper()
	got, err := readResponse(doPost(ts, "/?f=csv", query, options...))
	if err != nil {
		t.Error(err)
		return
	}
	assertEqual(t, want, got)
}

//////////////////////////////////////////////////////////////////////////////
// Test cases

func TestCheckDB(t *testing.T) {
	err := checkDB(t.Context())
	if err != nil {
		t.Error(err)
	}
}

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

// TestQueryStats contains query statistics.
type TestQueryStats struct {
	ID       string `json:"ID"`
	ConnID   string `json:"ConnID"`
	Query    string `json:"Query"`
	Start    string `json:"Start"`
	Duration string `json:"Duration"`
}

func TestCancelQuery(t *testing.T) {
	ts := startServer0(t)
	t.Run("canceled", func(t *testing.T) {
		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			// A slow query, to be interrupted
			r, err := doPost(ts, "/", `SELECT count(md5(i::VARCHAR)) as count_md5 FROM range(0, 100000000, 1) t1(i)`)
			const want = "context canceled\nINTERRUPT Error: Interrupted!\n"
			got, err := readResponse2(r, err, 504, 504)
			if err != nil {
				t.Errorf("slow query failed: %s", err)
			}
			assertEqual(t, want, got)
		}()
		time.Sleep(100 * time.Millisecond)
		// List executing queries
		queries, err := readJSONL[TestQueryStats](doGet(ts, "/status/queries/"))
		if err != nil {
			t.Error(err)
			return
		}
		// Interrupt (DELETE) a query
		if len(queries) != 1 {
			t.Errorf("unexpected number of queries: %d", len(queries))
			return
		}
		r, err := doDelete(ts, "/status/queries/"+queries[0].ID)
		got, err := readResponse2(r, err, 204, 204)
		if err != nil {
			t.Error(err)
		}
		assertEqual(t, "", got)
		wg.Wait()
	})
	t.Run("not found", func(t *testing.T) {
		r, err := doDelete(ts, "/status/queries/Q_deadbeaf")
		got, err := readResponse2(r, err, 404, 404)
		if err != nil {
			t.Error(err)
		}
		assertEqual(t, "Not Found\n", got)
	})
}

func testAuthorizedQuery(t *testing.T, ts *httptest.Server, query, want string, wantAuthID *string, options ...RequestOption) {
	t.Helper()
	resp, err := doPost(ts, "/?f=csv", query, options...)
	got, err := readResponse(resp, err)
	if err != nil {
		t.Errorf("request failed: %s", err)
		return
	}
	assertEqual(t, want, got)
	if wantAuthID == nil {
		if s, ok := resp.Header[AuthnIDHeader]; ok {
			t.Errorf("unexpected authn ID provided: %s", s)
		}
		return
	}
	gotAuthID, ok := resp.Header[AuthnIDHeader]
	if !ok || len(gotAuthID) == 0 {
		t.Error("unavailable authnID")
		return
	}
	assertEqual(t, *wantAuthID, gotAuthID[0])
}

func testUnauthorizedQuery(t *testing.T, ts *httptest.Server, query string, options ...RequestOption) {
	t.Helper()
	resp, err := doPost(ts, "/?f=csv", query, options...)
	got, err := readResponse2(resp, err, 401, 401)
	if err != nil {
		t.Errorf("request failed: %s", err)
		return
	}
	assertEqual(t, "Unauthorized\n", got)
	if s, ok := resp.Header[AuthnIDHeader]; ok {
		t.Errorf("unexpected authn ID provided: %s", s)
	}
}

func TestAuthnQuery(t *testing.T) {
	t.Run("authorized", func(t *testing.T) {
		ts := startServer0(t)
		setupAuthn(t, "testdata/authn.json", false)
		testAuthorizedQuery(t, ts, `SELECT version() AS V`, "V\nv1.5.0\n", new("token1"), authorizationBearer("token-0123456789abcdef"))
		testAuthorizedQuery(t, ts, `SELECT version() AS V`, "V\nv1.5.0\n", new("token2"), authorizationBearer("foobarbaz"))
		testAuthorizedQuery(t, ts, `SELECT version() AS V`, "V\nv1.5.0\n", new("user1"), authorizationBasic("user1", "abcd1234"))
		testAuthorizedQuery(t, ts, `SELECT version() AS V`, "V\nv1.5.0\n", new("user2"), authorizationBasic("user2", "xyz789"))
	})
	t.Run("not authorized", func(t *testing.T) {
		ts := startServer0(t)
		setupAuthn(t, "testdata/authn.json", false)
		testAuthorizedQuery(t, ts, `SELECT version() AS V`, "V\nv1.5.0\n", new("token1"), authorizationBearer("token-0123456789abcdef"))
		// Should be failed without Authorization header
		testUnauthorizedQuery(t, ts, `SELECT version() AS V`)
		// Should be failed for with wrong Authorization header
		testUnauthorizedQuery(t, ts, `SELECT version() AS V`, authorizationBearer("unknown-token"))
	})
	t.Run("without authorization", func(t *testing.T) {
		ts := startServer0(t)
		setupAuthn(t, "testdata/authn.json", true)
		testAuthorizedQuery(t, ts, `SELECT version() AS V`, "V\nv1.5.0\n", new("token1"), authorizationBearer("token-0123456789abcdef"))
		testAuthorizedQuery(t, ts, `SELECT version() AS V`, "V\nv1.5.0\n", nil)
	})
}

func TestAuthnInitQuery(t *testing.T) {
	ts := startServer0(t)
	setupAuthn(t, "testdata/authn.json", false)
	// Verify that the initial value of `threads` is 1.
	testAuthorizedQuery(t, ts, `SELECT current_setting('threads') AS T`, "T\n1\n", new("token1"), authorizationBearer("token-0123456789abcdef"))
	// Disconnect all connections and reset the DuckDB instance.
	ts.Client().Transport.(*http.Transport).CloseIdleConnections()
	// The initial value of threads is 1, but it is overwritten to 2 by authn's InitQuery.
	testAuthorizedQuery(t, ts, `SELECT current_setting('threads') AS T`, "T\n2\n", new("threads-2"), authorizationBearer("token-threads-2"))
	// Verify that there is no impact on authentication without an InitQuery.
	ts.Client().Transport.(*http.Transport).CloseIdleConnections()
	testAuthorizedQuery(t, ts, `SELECT current_setting('threads') AS T`, "T\n1\n", new("user1"), authorizationBasic("user1", "abcd1234"))
}

const (
	idSyntaxError = "ID syntax error: query ID should starts with \"Q_\"\n"
)

func testAuthorizedInterruptQuery(t *testing.T, ts *httptest.Server, queryID string, want string, wantAuthID *string, options ...RequestOption) {
	t.Helper()
	resp, err := doDelete(ts, "/status/queries/"+queryID, options...)
	got, err := readResponse2(resp, err, 400, 400)

	if err != nil {
		t.Errorf("request failed: %s", err)
		return
	}
	assertEqual(t, want, got)

	if wantAuthID == nil {
		if s, ok := resp.Header[AuthnIDHeader]; ok {
			t.Errorf("unexpected authn ID provided: %s", s)
		}
		return
	}
	gotAuthID, ok := resp.Header[AuthnIDHeader]
	if !ok || len(gotAuthID) == 0 {
		t.Error("unavailable authnID")
		return
	}
	assertEqual(t, *wantAuthID, gotAuthID[0])
}

func testUnauthorizedInterruptQuery(t *testing.T, ts *httptest.Server, queryID string, options ...RequestOption) {
	t.Helper()
	resp, err := doDelete(ts, "/status/queries/"+queryID, options...)
	got, err := readResponse2(resp, err, 401, 401)
	if err != nil {
		t.Errorf("request failed: %s", err)
		return
	}
	assertEqual(t, "Unauthorized\n", got)
	if s, ok := resp.Header[AuthnIDHeader]; ok {
		t.Errorf("unexpected authn ID provided: %s", s)
	}
}

func TestAuthnInterruptQuery(t *testing.T) {
	t.Run("authorized", func(t *testing.T) {
		ts := startServer0(t)
		setupAuthn(t, "testdata/authn.json", false)
		testAuthorizedInterruptQuery(t, ts, "dummy", idSyntaxError, new("token1"), authorizationBearer("token-0123456789abcdef"))
		testAuthorizedInterruptQuery(t, ts, "dummy", idSyntaxError, new("token2"), authorizationBearer("foobarbaz"))
		testAuthorizedInterruptQuery(t, ts, "dummy", idSyntaxError, new("user1"), authorizationBasic("user1", "abcd1234"))
		testAuthorizedInterruptQuery(t, ts, "dummy", idSyntaxError, new("user2"), authorizationBasic("user2", "xyz789"))
	})
	t.Run("not authorized", func(t *testing.T) {
		ts := startServer0(t)
		setupAuthn(t, "testdata/authn.json", false)
		testAuthorizedInterruptQuery(t, ts, "dummy", idSyntaxError, new("token1"), authorizationBearer("token-0123456789abcdef"))
		testUnauthorizedInterruptQuery(t, ts, "dummy")
		testUnauthorizedInterruptQuery(t, ts, "dummy", authorizationBearer("unknown-token"))
	})
	t.Run("without authorization", func(t *testing.T) {
		ts := startServer0(t)
		setupAuthn(t, "testdata/authn.json", true)
		testAuthorizedInterruptQuery(t, ts, "dummy", idSyntaxError, new("token1"), authorizationBearer("token-0123456789abcdef"))
		testAuthorizedInterruptQuery(t, ts, "dummy", idSyntaxError, nil)
	})
}
