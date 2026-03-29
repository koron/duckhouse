package markdown_test

import (
	"testing"

	"github.com/koron/duckhouse/internal/assert"
	"github.com/koron/duckhouse/internal/formatter/markdown"
	"github.com/koron/duckhouse/internal/formatter/formattertest"
)

func TestFactory(t *testing.T) {
	f := formattertest.Find[*markdown.Factory](t, "markdown")
	assert.Equal(t, "text/markdown", f.ContentType())
}
