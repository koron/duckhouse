// Package duckdbinit provides initializer for DuckDB instance.
package duckdbinit

import (
	"context"
	"database/sql"
)

type settingsKey struct{}

func GetSettings(ctx context.Context) *Settings {
	s, ok := ctx.Value(settingsKey{}).(*Settings)
	if !ok {
		return new(DefaultSettings)
	}
	return s
}

// initDB initializes a DB instance with parameters associated with the context.
func initDB(ctx context.Context, db *sql.DB) error {
	s := GetSettings(ctx)
	if err := s.apply(ctx, db); err != nil {
		return err
	}
	if InitQuery != "" {
		if _, err := db.ExecContext(ctx, InitQuery); err != nil {
			return err
		}
	}
	return nil
}

func Open(ctx context.Context) (*sql.DB, error) {
	s := GetSettings(ctx)
	// FIXME: more flexible DSN.
	db, err := sql.Open("duckdb", "?home_directory="+s.HomeDir)
	if err != nil {
		return nil, err
	}
	if err := initDB(ctx, db); err != nil {
		db.Close()
		return nil, err
	}
	return db, nil
}

type Settings struct {
	HomeDir string

	Threads        int
	MemoryLimit    string
	ExtensionDir   string
	SecretDir      string
	TempDir        string
	MaxTempDirSize string

	LockConfig bool
}

func (s *Settings) apply(ctx context.Context, db *sql.DB) error {
	// NOTE: home_directory show be specified in DSN
	// Limit the resources used by a DuckDB instance.
	if s.Threads != 0 {
		_, err := db.ExecContext(ctx, "SET threads = ?", s.Threads)
		if err != nil {
			return err
		}
	}
	if s.MemoryLimit != "" {
		_, err := db.ExecContext(ctx, "SET memory_limit = ?", s.MemoryLimit)
		if err != nil {
			return err
		}
	}
	if s.ExtensionDir != "" {
		_, err := db.ExecContext(ctx, "SET extension_directory = ?", s.ExtensionDir)
		if err != nil {
			return err
		}
	}
	if s.SecretDir != "" {
		_, err := db.ExecContext(ctx, "SET secret_directory = ?", s.SecretDir)
		if err != nil {
			return err
		}
	}
	if s.TempDir != "" {
		_, err := db.ExecContext(ctx, "SET temp_directory = ?", s.TempDir)
		if err != nil {
			return err
		}
	}
	if s.MaxTempDirSize != "" {
		_, err := db.ExecContext(ctx, "SET max_temp_directory_size = ?", s.MaxTempDirSize)
		if err != nil {
			return err
		}
	}
	if s.LockConfig {
		_, err := db.ExecContext(ctx, "SET lock_configuration = ?", s.LockConfig)
		if err != nil {
			return err
		}
	}
	return nil
}

var DefaultSettings Settings

var InitQuery string
