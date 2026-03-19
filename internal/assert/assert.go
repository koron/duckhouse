// Package assert provides assert functions for testing.
package assert

import (
	"testing"

	"github.com/google/go-cmp/cmp"
)

func Equal[T any](t *testing.T, want, got T, options ...cmp.Option) {
	t.Helper()
	if d := cmp.Diff(want, got, options...); d != "" {
		t.Errorf("assert failed, mismatch: -want +got\n%s", d)
	}
}
