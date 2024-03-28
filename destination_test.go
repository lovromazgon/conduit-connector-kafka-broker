package kafkabroker

import (
	"context"
	"testing"

	"github.com/matryer/is"
)

func TestTeardown_NoOpen(t *testing.T) {
	is := is.New(t)
	conn := NewDestination()
	err := conn.Teardown(context.Background())
	is.NoErr(err)
}
