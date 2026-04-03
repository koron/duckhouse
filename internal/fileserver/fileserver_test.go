package fileserver_test

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/koron/duckpop/internal/assert"
	"github.com/koron/duckpop/internal/fileserver"
)

func writeFile(t *testing.T, name, body string) {
	t.Helper()
	err := os.WriteFile(name, []byte(body), 0666)
	if err != nil {
		t.Errorf("os.WriteFile failure: %s", err)
	}
}

func doGet(ts *httptest.Server, name string) (string, error) {
	req, err := http.NewRequest("GET", ts.URL+"/"+name, nil)
	if err != nil {
		return "", err
	}
	resp, err := ts.Client().Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return "", fmt.Errorf("unexpected status code: want=200 got=%d", resp.StatusCode)
	}
	b, _ := io.ReadAll(resp.Body)
	if err != nil {
		return "", fmt.Errorf("read body failed: %w", err)
	}
	return string(b), nil
}

func doMkcol(ts *httptest.Server, name string) error {
	req, err := http.NewRequest("MKCOL", ts.URL+"/"+name, nil)
	if err != nil {
		return err
	}
	resp, err := ts.Client().Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 201 {
		return fmt.Errorf("unexpected status code: want=201 got=%d", resp.StatusCode)
	}
	return nil
}

func doDelete(ts *httptest.Server, name string) error {
	req, err := http.NewRequest("DELETE", ts.URL+"/"+name, nil)
	if err != nil {
		return err
	}
	resp, err := ts.Client().Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 204 {
		return fmt.Errorf("unexpected status code: want=204 got=%d", resp.StatusCode)
	}
	return nil
}

func doPut(ts *httptest.Server, name, content string) error {
	req, err := http.NewRequest("PUT", ts.URL+"/"+name, strings.NewReader(content))
	if err != nil {
		return err
	}
	resp, err := ts.Client().Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 201 {
		return fmt.Errorf("unexpected status code: want=201 got=%d", resp.StatusCode)
	}
	return nil
}

func TestServer(t *testing.T) {
	tmpdir := t.TempDir()
	ts := httptest.NewServer(fileserver.New(tmpdir))
	t.Cleanup(ts.Close)

	writeFile(t, filepath.Join(tmpdir, "foo.txt"), "foo\n")
	writeFile(t, filepath.Join(tmpdir, "bar.txt"), "bar\n")

	t.Run("GET", func(t *testing.T) {
		got1, err := doGet(ts, "foo.txt")
		if err != nil {
			t.Error(err)
		}
		assert.Equal(t, "foo\n", got1)
		got2, err := doGet(ts, "bar.txt")
		if err != nil {
			t.Error(err)
		}
		assert.Equal(t, "bar\n", got2)
	})

	t.Run("MKCOL", func(t *testing.T) {
		err := doMkcol(ts, "testdir1")
		if err != nil {
			t.Errorf("MKCOL testdir1 failed: %s", err)
		}
		assert.IsDir(t, filepath.Join(tmpdir, "testdir1"))
	})

	t.Run("DELETE", func(t *testing.T) {
		name := filepath.Join(tmpdir, "delete.txt")
		writeFile(t, name, "Hello World\n")
		err := doDelete(ts, "delete.txt")
		if err != nil {
			t.Errorf("DELETE delete.txt failed: %s", err)
		}
		assert.IsNotExist(t, name)
	})

	t.Run("PUT", func(t *testing.T) {
		name := filepath.Join(tmpdir, "put1.txt")
		err := doPut(ts, "put1.txt", "Hello Put1\n")
		if err != nil {
			t.Errorf("PUT put1.txt failed: %s", err)
		}
		assert.ReadFile(t, name, "Hello Put1\n")

		err = doPut(ts, "put1.txt", "Hello Put2\n")
		if err != nil {
			t.Errorf("PUT put1.txt failed: %s", err)
		}
		assert.ReadFile(t, name, "Hello Put2\n")
	})
}
