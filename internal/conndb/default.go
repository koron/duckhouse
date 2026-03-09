package conndb

import (
	"context"
	"database/sql"
	"net"
	"net/http"
)

var Default = &Manager{
	MaxDB: 4,
}

func SetMaxDB(n int) {
	Default.MaxDB = n
}

func ConnContext(ctx context.Context, c net.Conn) context.Context {
	return Default.ConnContext(ctx, c)
}

func ConnState(c net.Conn, s http.ConnState) {
	Default.ConnState(c, s)
}

func GetID(ctx context.Context) (ID, bool) {
	return Default.GetID(ctx)
}

func GetDB(ctx context.Context) (*sql.DB, ID, error) {
	return Default.GetDB(ctx)
}
