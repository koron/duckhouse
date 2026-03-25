package pidfile_test

import (
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/koron/duckhouse/internal/assert"
	"github.com/koron/duckhouse/internal/pidfile"
)

func TestSingle(t *testing.T) {
	tmpdir := t.TempDir()
	name := filepath.Join(tmpdir, "test.pid")

	err := pidfile.Write(name)
	if err != nil {
		t.Fatal(err)
	}

	got, err := os.ReadFile(name)
	if err != nil {
		t.Error(err)
	}
	want := strconv.Itoa(os.Getpid())
	assert.Equal(t, want, string(got))

	pidfile.Close()
	assert.IsNotExist(t, name)
}

func TestMultiple(t *testing.T) {
	tmpdir := t.TempDir()
	names := []string{
		filepath.Join(tmpdir, "test1.pid"),
		filepath.Join(tmpdir, "test2.pid"),
		filepath.Join(tmpdir, "test3.pid"),
	}

	for _, name := range names {
		err := pidfile.Write(name)
		if err != nil {
			t.Errorf("failed to write: name=%s: %s", name, err)
		}
	}
	if t.Failed() {
		return
	}

	for _, name := range names {
		got, err := os.ReadFile(name)
		if err != nil {
			t.Errorf("failed to read: name=%s: %s", name, err)
		}
		want := strconv.Itoa(os.Getpid())
		assert.Equal(t, want, string(got))
	}
	if t.Failed() {
		return
	}

	pidfile.Close()
	for _, name := range names {
		assert.IsNotExist(t, name)
	}
}
