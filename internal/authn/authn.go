package authn

import (
	"context"
	"net/http"
)

type ID string

var NoAuthn ID = ""

func extractID(r *http.Request) (ID, error) {
	// TODO:
	return "", nil
}

type idKey = struct{}

func WrapHandler(h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Embed authenticity information to request context.
		id, err := extractID(r)
		if err == nil {
			ctx := context.WithValue(r.Context(), idKey{}, id)
			r = r.WithContext(ctx)
		}
		h.ServeHTTP(w, r)
	})
	return nil
}

func AuthnID(r *http.Request) ID {
	id, ok := r.Context().Value(idKey{}).(ID)
	if !ok {
		id = NoAuthn
	}
	return id
}
