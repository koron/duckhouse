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

// Init initializes a DB instance with parameters associated with the context.
func Init(ctx context.Context, db *sql.DB) error {
	s := GetSettings(ctx)
	return s.Apply(ctx, db)
}

type Settings struct {
	Threads     *int
	MemoryLimit *string
}

func (s *Settings) Apply(ctx context.Context, db *sql.DB) error {
	// Limit the resources used by a DuckDB instance.
	if s.Threads != nil {
		_, err := db.ExecContext(ctx, "SET threads = ?", *s.Threads)
		if err != nil {
			return err
		}
	}
	if s.MemoryLimit != nil {
		_, err := db.ExecContext(ctx, "SET memory_limit = ?", *s.MemoryLimit)
		if err != nil {
			return err
		}
	}
	return nil
}

var DefaultSettings Settings
