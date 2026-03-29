package avro_test

import (
	"testing"

	"github.com/koron/duckhouse/internal/assert"
	"github.com/koron/duckhouse/internal/formatter/avro"
	"github.com/koron/duckhouse/internal/formatter/formattertest"
)

func TestFactory(t *testing.T) {
	f := formattertest.Find[*avro.Factory](t, "avro")
	assert.Equal(t, "application/avro", f.ContentType())
}
