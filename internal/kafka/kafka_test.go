package kafka

import (
	"context"
	"encoding/hex"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/matryer/is"
	"github.com/rs/zerolog"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func TestKafka(t *testing.T) {
	replica := NewReplica("localhost", 9092)
	handler := NewSourceRequestHandler(replica)
	server := NewServer(handler, zerolog.New(zerolog.NewTestWriter(t)))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := server.Run(ctx, "localhost:9092")
		if err != nil {
			t.Fatal(err)
		}
	}()

	time.Sleep(time.Second)

	is := is.New(t)

	client, err := kgo.NewClient(
		kgo.SeedBrokers("localhost:9092"),
		kgo.ClientID("foo"),
		kgo.DefaultProduceTopic("test-topic"),
		kgo.AllowAutoTopicCreation(),
	)
	is.NoErr(err)

	resp, err := client.Request(ctx, &kmsg.MetadataRequest{
		Version:                4,
		AllowAutoTopicCreation: true,
		Topics: []kmsg.MetadataRequestTopic{{
			TopicID: [16]byte{},
			Topic:   kmsg.StringPtr("test-topic"),
		}},
	})
	is.NoErr(err)
	t.Log(resp)

	var batch Batch
	go func() {
		batch = <-replica.Queues()[0]
	}()

	t.Log("PRODUCING SYNC")
	results := client.ProduceSync(ctx, &kgo.Record{
		Key:   []byte("hello"),
		Value: []byte("world"),
	})
	is.NoErr(results.FirstErr())

	t.Log(batch)

	t.Log("DONE")
	cancel()
	wg.Wait()
}

/*
00000000  00 00 00 23 00 12 00 03  00 00 00 00 00 0b 74 65  |...#..........te|
00000010  73 74 2d 63 6c 69 65 6e  74 00 04 6b 67 6f 08 75  |st-client..kgo.u|
00000020  6e 6b 6e 6f 77 6e 00                              |nknown.|


00000000  00 04 6b 67 6f 08 75 6e  6b 6e 6f 77 6e 00        |..kgo.unknown.|
*/

/* ---------------- NEED ------------------------------------
00000000  04 6b 67 6f 08 75 6e 6b  6e 6f 77 6e 00           |.kgo.unknown.|
*/

func TestApiRequest(t *testing.T) {
	req := kmsg.ApiVersionsRequest{
		Version:               3,
		ClientSoftwareName:    "kgo",
		ClientSoftwareVersion: "unknown",
	}
	buf := req.AppendTo(nil)
	fmt.Println(hex.Dump(buf))

	var req2 kmsg.ApiVersionsRequest
	req2.SetVersion(3)
	err := req2.ReadFrom(buf)
	fmt.Println(err)

	fmt.Println(req2)
}
