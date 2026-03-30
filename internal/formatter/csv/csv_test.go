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

func TestFactory(t *testing.T) {
	f := formattertest.Find[*csv.Factory](t, format)
	assert.Equal(t, "text/csv", f.ContentType())
}

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

// Tests

func TestParamNull(t *testing.T) {
	conn := formattertest.ConnectDB(t)
	runCases(t, conn, "csv", []testCase{
		{`SELECT NULL AS GOT`, "GOT\nNULL\n"},
	})
	runCases(t, conn, "csv,null:(null)", []testCase{
		{`SELECT NULL AS GOT`, "GOT\n(null)\n"},
	})
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

func TestIntegerTypes(t *testing.T) {
	conn := formattertest.ConnectDB(t)
	runCases(t, conn, format, []testCase{
		{`SELECT -5::BIGINT AS V`, "V\n-5\n"},   // INT8
		{`SELECT -4::HUGEINT AS V`, "V\n-4\n"},  // INT16
		{`SELECT -3::INTEGER AS V`, "V\n-3\n"},  // INT4
		{`SELECT -2::SMALLINT AS V`, "V\n-2\n"}, // INT2
		{`SELECT -1::TINYINT AS V`, "V\n-1\n"},  // INT1
		{`SELECT 1::UBIGINT AS V`, "V\n1\n"},
		{`SELECT 2::UHUGEINT AS V`, "V\n2\n"},
		{`SELECT 3::UINTEGER AS V`, "V\n3\n"},
		{`SELECT 4::USMALLINT AS V`, "V\n4\n"},
		{`SELECT 5::UTINYINT AS V`, "V\n5\n"},
	})
}

func TestFloatTypes(t *testing.T) {
	conn := formattertest.ConnectDB(t)
	runCases(t, conn, format, []testCase{
		{`SELECT (1/2)::FLOAT AS V`, "V\n0.5\n"},
		{`SELECT (1/2)::DOUBLE AS V`, "V\n0.5\n"},
	})
}
