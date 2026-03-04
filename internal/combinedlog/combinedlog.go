package combinedlog

import (
	"fmt"
	"io"
	"net/http"
	"time"
)

type wrapWriter struct {
	base   http.ResponseWriter
	status int
	bsize  int
}

func (w *wrapWriter) Header() http.Header {
	return w.base.Header()
}

func (w *wrapWriter) Write(data []byte) (int, error) {
	n, err := w.base.Write(data)
	w.bsize += n
	return n, err
}

func (w *wrapWriter) WriteHeader(statusCode int) {
	w.base.WriteHeader(statusCode)
	w.status = statusCode
}

func writeCombinedLog(w io.Writer, r *http.Request, status, bodySize int) {
	remoteAddr := r.RemoteAddr
	ident := "-"
	user := "-"
	if r.URL.User != nil {
		user = r.URL.User.Username()
	}
	timestamp := time.Now().Format("02/Jan/2006:15:04:05 -0700")
	requestLine := fmt.Sprintf("%s %s %s", r.Method, r.URL.RequestURI(), r.Proto)
	referer := r.Referer()
	if referer == "" {
		referer = "-"
	}
	userAgent := r.UserAgent()
	if userAgent == "" {
		userAgent = "-"
	}
	fmt.Fprintf(w, "%s %s %s [%s] \"%s\" %d %d \"%s\" \"%s\"\n",
		remoteAddr, ident, user, timestamp, requestLine, status, bodySize, referer, userAgent)
}

func WrapHandler(logWriter io.Writer, h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ww := &wrapWriter{base: w}
		h.ServeHTTP(ww, r)
		writeCombinedLog(logWriter, r, ww.status, ww.bsize)
	})
	return nil
}
