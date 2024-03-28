package kafka-broker_test

import (
	"context"
	"testing"

	kafka-broker "github.com/lovromazgon/conduit-connector-kafka-broker"
	"github.com/matryer/is"
)

func TestTeardown_NoOpen(t *testing.T) {
	is := is.New(t)
	con := kafka-broker.NewDestination()
	err := con.Teardown(context.Background())
	is.NoErr(err)
}
