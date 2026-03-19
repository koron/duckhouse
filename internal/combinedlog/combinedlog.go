// Package combinedlog provides access log for duckhouse
package combinedlog

import (
	"log/slog"
	"net/http"
	"time"

	"github.com/koron/duckhouse/internal/authn"
	"github.com/koron/duckhouse/internal/conndb"
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

func writeLog(logger *slog.Logger, ww *wrapWriter, r *http.Request) {
	// Basic information: remote, authn
	authnID, ok := authn.AuthnID(r)
	authnIDStr := "-"
	if ok {
		authnIDStr = authnID.String()
	}

	// Request information: referer, user-agent
	referer := r.Referer()
	if referer == "" {
		referer = "-"
	}
	userAgent := r.UserAgent()
	if userAgent == "" {
		userAgent = "-"
	}

	// Connection and query
	connID := "-"
	if cid, ok := conndb.GetID(r.Context()); ok {
		connID = cid.String()
	}

	attrs := []slog.Attr{
		slog.String("remote_addr", r.RemoteAddr),
		slog.String("authn_id", authnIDStr),
		slog.String("method", r.Method),
		slog.String("path", r.URL.RequestURI()),
		slog.String("proto", r.Proto),
		slog.Int("status", ww.status),
		slog.Int("size", ww.bsize),
		slog.String("referer", referer),
		slog.String("user_agent", userAgent),
		slog.String("conn_id", connID),
	}

	if ww.queryReport != nil {
		attrs = append(attrs,
			slog.String("query", ww.queryReport.query),
			slog.Duration("duration", ww.queryReport.duration),
		)
	}

	logger.LogAttrs(r.Context(), slog.LevelInfo, "access", attrs...)
}

func WrapHandler(logger *slog.Logger, h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ww := &wrapWriter{base: w}
		h.ServeHTTP(ww, r)
		writeLog(logger, ww, r)
	})
}
