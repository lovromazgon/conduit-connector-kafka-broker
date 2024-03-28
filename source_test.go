package kafkabroker

import (
	"context"
	"testing"

	"github.com/matryer/is"
)

func TestTeardownSource_NoOpen(t *testing.T) {
	is := is.New(t)
	conn := NewSource()
	err := conn.Teardown(context.Background())
	is.NoErr(err)
}
