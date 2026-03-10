package table

import (
	"database/sql"
	"io"

	"github.com/koron/duckhouse/internal/formatter"
	"github.com/olekukonko/tablewriter"
)

func init() {
	formatter.Register(&Factory{}, "table")
}

type Factory struct {
}

var _ formatter.Factory = (*Factory)(nil)

func (f *Factory) ContentType() string {
	return "text/plain"
}

func (f *Factory) Create(w io.Writer, params map[string]string) (formatter.Writer, error) {
	// FIXME: Apply params
	return &Writer{
		t: tablewriter.NewTable(w),
	}, nil
}

type Writer struct {
	t *tablewriter.Table
}

var _ formatter.Writer = (*Writer)(nil)

func (w *Writer) WriteHeader(columnTypes []*sql.ColumnType) error {
	elements := make([]string, len(columnTypes))
	for i, typ := range columnTypes {
		elements[i] = typ.Name()
	}
	w.t.Header(elements)
	return nil
}

func (w *Writer) WriteBody(values []any) error {
	return w.t.Append(values)
}

func (w *Writer) Flush() error {
	return w.t.Render()
}
