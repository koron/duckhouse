package conndb

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"net"
	"net/http"
	"sync"
)

type Manager struct {
	MaxDB int

	idSet    sync.Map
	connToID sync.Map
	idToDB   sync.Map

	dbCount int
	dbMutex sync.Mutex
}

type ID uint64

func (id ID) String() string {
	return fmt.Sprintf("%016x", uint64(id))
}

func (m *Manager) newID(c net.Conn) ID {
	for {
		id := ID(rand.Uint64())
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

func (m *Manager) closeConn(c net.Conn) error {
	rawid, ok := m.connToID.LoadAndDelete(c)
	if !ok {
		return fmt.Errorf("no ID for net.Conn=%p", c)
	}
	m.dbMutex.Lock()
	defer m.dbMutex.Unlock()
	id := rawid.(ID)
	m.idSet.Delete(id)
	rawdb, ok := m.idToDB.LoadAndDelete(id)
	if !ok {
		return nil
	}
	db := rawdb.(*sql.DB)
	db.Close()
	if m.dbCount > 0 {
		m.dbCount--
	}
	slog.Debug("DB closed", "connID", id, "DB", dbToStr(db), "count", m.dbCount)
	return nil
}

var (
	ErrNoConnection = errors.New("no connections assigned for the context")
	ErrMaxDB        = errors.New("reached maximum number of DB")
)

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
	db, err := sql.Open("duckdb", "")
	if err != nil {
		return nil, 0, err
	}
	db.SetMaxIdleConns(0)
	m.idToDB.Store(id, db)
	m.dbCount++
	slog.Debug("DB opened", "connID", id, "DB", dbToStr(db), "count", m.dbCount)
	return db, id, nil
}
