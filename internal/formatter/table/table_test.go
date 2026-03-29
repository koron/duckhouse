package table_test

import (
	"testing"

	"github.com/koron/duckhouse/internal/assert"
	"github.com/koron/duckhouse/internal/formatter/formattertest"
	"github.com/koron/duckhouse/internal/formatter/table"
)

func TestFactory(t *testing.T) {
	f := formattertest.Find[*table.Factory](t, "table")
	assert.Equal(t, "text/plain", f.ContentType())
}
