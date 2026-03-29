package html_test

import (
	"testing"

	"github.com/koron/duckhouse/internal/assert"
	"github.com/koron/duckhouse/internal/formatter/html"
	"github.com/koron/duckhouse/internal/formatter/formattertest"
)

func TestFactory(t *testing.T) {
	f := formattertest.Find[*html.Factory](t, "html")
	assert.Equal(t, "text/html", f.ContentType())
}
