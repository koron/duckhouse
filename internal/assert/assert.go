// Package assert provides assert functions for testing.
package assert

import (
	"os"
	"testing"

	"github.com/google/go-cmp/cmp"
)

func Equal[T any](t *testing.T, want, got T, options ...cmp.Option) {
	t.Helper()
	if d := cmp.Diff(want, got, options...); d != "" {
		t.Errorf("assert failed, mismatch: -want +got\n%s", d)
	}
}

func IsRegularFile(t *testing.T, name string) {
	t.Helper()
	fi, err := os.Stat(name)
	if err != nil {
		t.Errorf("failed to stat: %s", err)
		return
	}
	if !fi.Mode().IsRegular() {
		t.Errorf("not regular file: %s", name)
		return
	}
}

func IsNotExist(t *testing.T, name string) {
	t.Helper()
	_, err := os.Stat(name)
	if err == nil {
		t.Errorf("a file exists unexpectedly: %s", name)
		return
	}
	if !os.IsNotExist(err) {
		t.Errorf("unexpected error, want fs.ErrNotExist: got=%s", err)
		return
	}
}
