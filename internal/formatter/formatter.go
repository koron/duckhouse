package formatter

import (
	"database/sql"
	"errors"
	"fmt"
	"io"
	"strings"
)

type Factory interface {
	ContentType() string
	Create(w io.Writer, params map[string]string) (Writer, error)
}

type Writer interface {
	WriteHeader(columnTypes []*sql.ColumnType) error
	WriteBody(values []any) error
	Flush() error
}

var factories = map[string]Factory{}

func Register(factory Factory, names ...string) {
	for _, name := range names {
		name = strings.ToLower(name)
		if _, ok := factories[name]; ok {
			panic(fmt.Sprintf("formatter %q is duplicated", name))
		}
		factories[name] = factory
	}
}

func Find(name string) (Factory, bool) {
	f, ok := factories[strings.ToLower(name)]
	return f, ok
}

var (
	ErrWithoutFactory  = errors.New("made without a factory")
	ErrNoHeaderWritten = errors.New("no headers written")
	ErrCountMismatch   = errors.New("header and body count mismatch")
)

func AnyToStr(v any) string {
	return fmt.Sprint(v)
}

func BlobToStr(v any) string {
	return string(v.([]uint8))
}

func Get(params map[string]string, name, defaultValue string) string {
	if s, ok := params[name]; ok {
		return s
	}
	return defaultValue
}
