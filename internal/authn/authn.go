// Package authn provides authentication information binding to the request.
package authn

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
)

type ID string

const NoAuthn ID = ""

func Enable() bool {
	return Default != nil
}

func extractID(r *http.Request) ID {
	if Default == nil {
		return ""
	}
	s := r.Header.Get("Authorization")
	e := Default.index[s]
	if e == nil {
		return ""
	}
	return e.ID
}

type idKey struct{}

func WrapHandler(h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Embed authenticity information to request context.
		id := extractID(r)
		if id != "" {
			ctx := context.WithValue(r.Context(), idKey{}, id)
			r = r.WithContext(ctx)
		}
		h.ServeHTTP(w, r)
	})
}

func AuthnID(r *http.Request) (ID, bool) {
	id, ok := r.Context().Value(idKey{}).(ID)
	if !ok {
		return NoAuthn, false
	}
	return id, true
}

type Type string

const (
	Basic  Type = "basic"
	Bearer Type = "bearer"
)

type User struct {
	Name     string `json:"name"`
	Password string `json:"password"`
}

type Entry struct {
	ID   ID   `json:"id"`
	Type Type `json:"type"`

	// Used for Basic type
	User *User `json:"user,omitempty"`

	// Used for Bearer type
	Token *string `json:"token,omitempty"`
}

func (e *Entry) headerValue() string {
	switch e.Type {
	case Basic:
		if e.User != nil {
			return "Basic " + base64.StdEncoding.EncodeToString([]byte(e.User.Name+":"+e.User.Password))
		}
	case Bearer:
		if e.Token != nil {
			return "Bearer " + strings.TrimSpace(*e.Token)
		}
	}
	return ""
}

var (
	Default *Authenticator
)

func ReadFile(name string) error {
	f, err := os.Open(name)
	if err != nil {
		return err
	}
	defer f.Close()
	a, err := readAuthenticator(f)
	if err != nil {
		return err
	}
	Default = a
	return nil
}

type Authenticator struct {
	entries []Entry
	index   map[string]*Entry
}

func readAuthenticator(r io.Reader) (*Authenticator, error) {
	var entries []Entry
	err := json.NewDecoder(r).Decode(&entries)
	if err != nil {
		return nil, err
	}
	idmap := map[ID]struct{}{}
	index := map[string]*Entry{}
	for i := range entries {
		e := &entries[i]
		// 1. Check for duplicate IDs.
		if _, ok := idmap[e.ID]; ok {
			return nil, fmt.Errorf("duplicated ID: %s", e.ID)
		}
		idmap[e.ID] = struct{}{}
		// 2. Check the type.
		if e.Type == Basic && e.User == nil {
			return nil, errors.New("required \"user\" property for \"basic\" type")
		}
		if e.Type == Bearer && e.Token == nil {
			return nil, errors.New("required \"token\" property for \"bearer\" type")
		}
		// 3. Create a reverse lookup index.
		x := e.headerValue()
		if x == "" {
			continue
		}
		index[x] = e
	}
	return &Authenticator{
		entries: entries,
		index:   index,
	}, nil
}
