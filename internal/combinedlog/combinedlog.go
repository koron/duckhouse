package combinedlog

import (
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/koron/duckhouse/internal/authn"
)

type QueryReporter interface {
	QueryReport(query string, duration time.Duration)
}

type wrapWriter struct {
	base   http.ResponseWriter
	status int
	bsize  int

	queryReport *queryReport
}

type queryReport struct {
	query    string
	duration time.Duration
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

func (w *wrapWriter) QueryReport(query string, duration time.Duration) {
	w.queryReport = &queryReport{
		query:    query,
		duration: duration,
	}
}

func writeLog(w io.Writer, ww *wrapWriter, r *http.Request) {
	remoteAddr := r.RemoteAddr
	authnID := authn.AuthnID(r)
	if authnID == authn.NoAuthn {
		authnID = "-"
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
	query := "-"
	duration := "-"
	if ww.queryReport != nil {
		query = ww.queryReport.query
		duration = ww.queryReport.duration.String()
	}
	fmt.Fprintf(w, "%s %s [%s] %q %q %q %d %d %q %s\n", remoteAddr, authnID, timestamp, requestLine, referer, userAgent, ww.status, ww.bsize, query, duration)
}

func WrapHandler(logWriter io.Writer, h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ww := &wrapWriter{base: w}
		h.ServeHTTP(ww, r)
		writeLog(logWriter, ww, r)
	})
	return nil
}
