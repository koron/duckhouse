package csv_test

import (
	"testing"

	"github.com/koron/duckhouse/internal/assert"
	"github.com/koron/duckhouse/internal/formatter/csv"
	"github.com/koron/duckhouse/internal/formatter/formattertest"
)

func TestFactory(t *testing.T) {
	f := formattertest.Find[*csv.Factory](t, "csv")
	assert.Equal(t, "text/csv", f.ContentType())
}

func TestTime(t *testing.T) {
	conn := formattertest.ConnectDB(t)
	bb := formattertest.Query(t, conn, "csv", `SELECT '12:34:56'::TIME AS VALUE`)
	assert.Equal(t, "VALUE\n12:34:56\n", bb.String())
}
