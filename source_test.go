package kafka-broker_test

import (
	"context"
	"testing"

	kafka-broker "github.com/lovromazgon/conduit-connector-kafka-broker"
	"github.com/matryer/is"
)

func TestTeardownSource_NoOpen(t *testing.T) {
	is := is.New(t)
	con := kafka-broker.NewSource()
	err := con.Teardown(context.Background())
	is.NoErr(err)
}
