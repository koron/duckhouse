package conndb

import (
	"context"
	"net"
	"net/http"
)

var Default = &Manager{
	MaxDB: 4,
}

func SetMaxDB(n int) {
	Default.MaxDB = n
}

func SetOpener(opener Opener) {
	Default.Opener = opener
}

func SetCloser(closer Closer) {
	Default.Closer = closer
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
