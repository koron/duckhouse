// Package duckdbinit provides initializer for DuckDB instance.
package duckdbinit

import (
	"context"
	"database/sql"
)

type settingsKey struct{}

func WithSettings(ctx context.Context, s Settings) context.Context {
	return context.WithValue(ctx, settingsKey{}, &s)
}

func GetSettings(ctx context.Context) *Settings {
	s, ok := ctx.Value(settingsKey{}).(*Settings)
	if !ok {
		return new(DefaultSettings)
	}
	return s
}

// initDB initializes a DB instance with parameters associated with the context.
func initDB(ctx context.Context, db *sql.DB, initQueries []string) error {
	s := GetSettings(ctx)
	if err := s.apply(ctx, db); err != nil {
		return err
	}
	for _, initQuery := range initQueries {
		if initQuery != "" {
			if _, err := db.ExecContext(ctx, initQuery); err != nil {
				return err
			}
		}
	}
	// Finally, configure lock_configuration.
	ex := &execContext{ctx: ctx, db: db}
	set(ex, "lock_configuration", s.LockConfig)
	return ex.err
}

func Open(ctx context.Context, initQueries ...string) (*sql.DB, error) {
	s := GetSettings(ctx)
	db, err := sql.Open("duckdb", "?home_directory="+s.HomeDir)
	if err != nil {
		return nil, err
	}
	if err := initDB(ctx, db, initQueries); err != nil {
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

// apply applies limits for the resources used by a DuckDB instance.
func (s *Settings) apply(ctx context.Context, db *sql.DB) error {
	// NOTE: home_directory show be specified in DSN
	ex := &execContext{ctx: ctx, db: db}
	set(ex, "threads", s.Threads)
	set(ex, "memory_limit", s.MemoryLimit)
	set(ex, "extension_directory", s.ExtensionDir)
	set(ex, "secret_directory", s.SecretDir)
	set(ex, "temp_directory", s.TempDir)
	set(ex, "max_temp_directory_size", s.MaxTempDirSize)
	return ex.err
}

type execContext struct {
	ctx context.Context
	db  *sql.DB
	err error
}

func set[T comparable](ex *execContext, name string, v T) {
	if ex.err != nil {
		return
	}
	var zero T
	if v == zero {
		return
	}
	_, err := ex.db.ExecContext(ex.ctx, "SET "+name+" = ?", v)
	ex.err = err
}

var DefaultSettings Settings
