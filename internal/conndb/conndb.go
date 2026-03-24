// Package conndb provides a per-connection database instance.
package conndb

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"iter"
	"log/slog"
	"math/rand/v2"
	"net"
	"net/http"
	"sync"
)

type Manager struct {
	MaxDB  int
	Opener Opener
	Closer Closer

	idSet    sync.Map
	connToID sync.Map
	idToDB   sync.Map

	dbCount int
	dbMutex sync.Mutex
}

type Opener interface {
	Open(ctx context.Context) (*sql.DB, error)
}

type OpenerFunc func(ctx context.Context) (*sql.DB, error)

func (fn OpenerFunc) Open(ctx context.Context) (*sql.DB, error) {
	return fn(ctx)
}

type Closer interface {
	Close(ctx context.Context, db *sql.DB) error
}

type CloserFunc func(ctx context.Context, db *sql.DB) error

func (fn CloserFunc) Close(ctx context.Context, db *sql.DB) error {
	return fn(ctx, db)
}

type ID uint32

func (id ID) String() string {
	return fmt.Sprintf("C_%08x", uint32(id))
}

func (m *Manager) newID(c net.Conn) ID {
	for {
		id := ID(rand.Uint32())
		_, ok := m.idSet.LoadOrStore(id, true)
		if !ok {
			m.connToID.Store(c, id)
			return id
		}
	}
}

type connIDKey = struct{}

func (m *Manager) ConnContext(ctx context.Context, c net.Conn) context.Context {
	id := m.newID(c)
	return context.WithValue(ctx, connIDKey{}, id)
}

func (m *Manager) ConnState(c net.Conn, s http.ConnState) {
	if s == http.StateClosed {
		err := m.closeConn(c)
		if err != nil {
			slog.Warn("failed to close DB", "error", err)
		}
	}
}

func dbToStr(db *sql.DB) string {
	return fmt.Sprintf("%p", db)
}

func (m *Manager) closeDB(db *sql.DB, id ID) error {
	ctx := context.WithValue(context.Background(), connIDKey{}, id)
	if m.Closer == nil {
		return db.Close()
	}
	return m.Closer.Close(ctx, db)
}

func (m *Manager) closeConn(c net.Conn) error {
	rawid, ok := m.connToID.LoadAndDelete(c)
	if !ok {
		return fmt.Errorf("no ID for net.Conn=%p", c)
	}

	m.dbMutex.Lock()
	id := rawid.(ID)
	m.idSet.Delete(id)
	rawdb, ok := m.idToDB.LoadAndDelete(id)
	if !ok {
		m.dbMutex.Unlock()
		return nil
	}
	if m.dbCount > 0 {
		m.dbCount--
	}
	count := m.dbCount
	m.dbMutex.Unlock()

	go func(db *sql.DB) {
		err := m.closeDB(db, id)
		if err != nil {
			slog.Warn("failed to close DB", "connID", id, "error", err)
		}
		slog.Debug("DB closed", "connID", id, "DB", dbToStr(db), "count", count)
	}(rawdb.(*sql.DB))

	return nil
}

var (
	ErrNoConnection = errors.New("no connections assigned for the context")
	ErrMaxDB        = errors.New("reached maximum number of DB")
	ErrNoOpener     = errors.New("no Opener specified")
)

func (m *Manager) GetID(ctx context.Context) (ID, bool) {
	id, ok := ctx.Value(connIDKey{}).(ID)
	return id, ok
}

func (m *Manager) GetDB(ctx context.Context) (*sql.DB, ID, error) {
	id, ok := ctx.Value(connIDKey{}).(ID)
	if !ok {
		return nil, 0, ErrNoConnection
	}
	rawdb, ok := m.idToDB.Load(id)
	if ok {
		return rawdb.(*sql.DB), id, nil
	}
	// Create a new database for the connection
	m.dbMutex.Lock()
	defer m.dbMutex.Unlock()
	if m.dbCount >= m.MaxDB {
		return nil, 0, ErrMaxDB
	}
	if m.Opener == nil {
		return nil, 0, ErrNoOpener
	}
	db, err := m.Opener.Open(ctx)
	if err != nil {
		return nil, 0, err
	}
	db.SetMaxIdleConns(0)
	m.idToDB.Store(id, db)
	m.dbCount++
	slog.Debug("DB opened", "connID", id, "DB", dbToStr(db), "count", m.dbCount)
	return db, id, nil
}

func (m *Manager) Databases() iter.Seq2[ID, *sql.DB] {
	return func(yield func(ID, *sql.DB) bool) {
		m.idToDB.Range(func(key, value any) bool {
			return yield(key.(ID), value.(*sql.DB))
		})
	}
}
