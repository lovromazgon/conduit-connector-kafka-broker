package kafkabroker

import (
	"context"
	"strconv"
	"testing"

	sdk "github.com/conduitio/conduit-connector-sdk"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/matryer/is"
)

func TestSource(t *testing.T) {
	is := is.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	conn := NewSource()

	err := conn.Configure(ctx, map[string]string{
		"addr": "localhost:9092",
	})
	is.NoErr(err)

	err = conn.Open(ctx, nil)
	is.NoErr(err)

	const recordCount = 10
	go func() {
		client, err := kgo.NewClient(
			kgo.SeedBrokers("localhost:9092"),
			kgo.ClientID("foo"),
			kgo.DefaultProduceTopic("test-topic"),
			kgo.AllowAutoTopicCreation(),
		)
		is.NoErr(err)

		for i := 0; i < recordCount; i++ {
			results := client.ProduceSync(ctx, &kgo.Record{
				Key:   []byte("hello" + strconv.Itoa(i)),
				Value: []byte("world" + strconv.Itoa(i)),
			})
			is.NoErr(results.FirstErr())
		}

		client.Close()
	}()

	for i := 0; i < recordCount; i++ {
		rec, err := conn.Read(ctx)
		is.NoErr(err)

		is.Equal(rec.Key, sdk.RawData("hello"+strconv.Itoa(i)))
		is.Equal(rec.Payload.After, sdk.RawData("world"+strconv.Itoa(i)))
		is.Equal(rec.Metadata["kafkabroker.topic"], "test-topic")
	}

	cancel()
	err = conn.Teardown(context.Background())
	is.NoErr(err)
}
