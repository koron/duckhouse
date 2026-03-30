package csv_test

import (
	"database/sql"
	"testing"

	"github.com/koron/duckhouse/internal/assert"
	"github.com/koron/duckhouse/internal/formatter/csv"
	"github.com/koron/duckhouse/internal/formatter/formattertest"
)

const (
	format = "csv"
)

type testCase struct {
	Query string
	Want  string
}

func runCases(t *testing.T, conn *sql.Conn, format string, cases []testCase) {
	t.Helper()
	for i, tc := range cases {
		bb := formattertest.Query(t, conn, format, tc.Query)
		if !assert.Equal(t, tc.Want, bb.String()) {
			t.Logf("failed #%d case: query=%q", i, tc.Query)
		}
	}
}

func TestFactory(t *testing.T) {
	f := formattertest.Find[*csv.Factory](t, format)
	assert.Equal(t, "text/csv", f.ContentType())
}

func TestDate(t *testing.T) {
	conn := formattertest.ConnectDB(t)
	bb := formattertest.Query(t, conn, format, `SELECT '2026-03-30'::DATE AS GOT`)
	assert.Equal(t, "GOT\n2026-03-30\n", bb.String())
}

func TestInterval(t *testing.T) {
	conn := formattertest.ConnectDB(t)
	runCases(t, conn, format, []testCase{
		{`SELECT to_days(10) AS GOT`, "GOT\n10d\n"},
		{`SELECT TIMESTAMP '2025-12-31' - TIMESTAMP '2025-01-01' AS diff`, "diff\n364d\n"},
		{`SELECT age(TIMESTAMP '2025-12-31', TIMESTAMP '2025-01-01') AS diff`, "diff\n11mo 30d\n"},
		{`SELECT age(DATE '2025-10-24', DATE '1944-10-15') AS diff`, "diff\n81y 9d\n"},
		{`SELECT INTERVAL '3' HOUR AS interval`, "interval\n3h\n"},
		{`SELECT INTERVAL '1' HOUR + INTERVAL '23' MINUTE + INTERVAL '34' SECOND AS interval`, "interval\n1h 23m 34s\n"},
	})
}

func TestTime(t *testing.T) {
	conn := formattertest.ConnectDB(t)
	bb := formattertest.Query(t, conn, format, `SELECT '12:34:56'::TIME AS GOT`)
	assert.Equal(t, "GOT\n12:34:56\n", bb.String())
}

func TestTimestampTZ(t *testing.T) {
	conn := formattertest.ConnectDB(t)
	bb := formattertest.Query(t, conn, format, `SELECT '2026-03-30 12:34:56+09:00'::TIMESTAMPTZ AS GOT`)
	assert.Equal(t, "GOT\n2026-03-30 03:34:56 +0000 UTC\n", bb.String())
}

func TestTimestamp(t *testing.T) {
	conn := formattertest.ConnectDB(t)
	bb := formattertest.Query(t, conn, format, `SELECT '2026-03-30 12:34:56'::TIMESTAMP AS GOT`)
	assert.Equal(t, "GOT\n2026-03-30 12:34:56\n", bb.String())
}
