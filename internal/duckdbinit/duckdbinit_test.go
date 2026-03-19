package duckdbinit_test

import (
	"database/sql"
	"testing"

	_ "github.com/duckdb/duckdb-go/v2"
	"github.com/koron/duckhouse/internal/assert"
	"github.com/koron/duckhouse/internal/duckdbinit"
)

func testSetting[T any](t *testing.T, db *sql.DB, name string, want T) {
	t.Helper()
	var got T
	err := db.QueryRowContext(t.Context(), "SELECT current_setting(?)", name).Scan(&got)
	if err != nil {
		t.Errorf("failed to retrieve threads setting: %s", err)
		return
	}
	assert.Equal(t, want, got)
}

func TestDefaultSettings(t *testing.T) {
	duckdbinit.DefaultSettings = duckdbinit.Settings{
		Threads:     3,
		MemoryLimit: "2GiB",
		LockConfig:  true,
	}
	db, err := duckdbinit.Open(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		db.Close()
	})
	testSetting(t, db, "threads", 3)
	testSetting(t, db, "memory_limit", "2.0 GiB")
	testSetting(t, db, "lock_configuration", true)
}

func TestLockConfig(t *testing.T) {
	t.Run("true", func(t *testing.T) {
		ctx := duckdbinit.WithSettings(t.Context(), duckdbinit.Settings{
			Threads:    1,
			LockConfig: true,
		})
		duckdbinit.InitQuery = `SET threads = 4`
		db, err := duckdbinit.Open(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()

		// Verify that InitQuery is overwriting Settings.
		testSetting(t, db, "threads", 4)
		if t.Failed() {
			return
		}

		// Verify that changes cannot be made after InitQuery.
		_, err = db.ExecContext(ctx, "SET threads = 8")
		assert.Equal(t, `Invalid Input Error: Cannot change configuration option "threads" - the configuration has been locked`, err.Error())
	})

	t.Run("false", func(t *testing.T) {
		ctx := duckdbinit.WithSettings(t.Context(), duckdbinit.Settings{
			Threads:    1,
			LockConfig: false,
		})
		duckdbinit.InitQuery = `SET threads = 4`
		db, err := duckdbinit.Open(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()

		// Verify that InitQuery is overwriting Settings.
		testSetting(t, db, "threads", 4)
		if t.Failed() {
			return
		}

		// Verify that changes can be made after InitQuery.
		_, err = db.ExecContext(ctx, "SET threads = 8")
		if err != nil {
			t.Fatalf("failed to set threads: %s", err)
		}
		testSetting(t, db, "threads", 8)
	})
}
