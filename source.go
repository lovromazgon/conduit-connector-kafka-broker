package kafkabroker

//go:generate paramgen -output=paramgen_src.go SourceConfig

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/twmb/franz-go/pkg/kbin"
	"github.com/twmb/franz-go/pkg/kmsg"

	"github.com/lovromazgon/conduit-connector-kafka-broker/internal/kafka"

	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/lovromazgon/conduit-connector-kafka-broker/internal"
)

type Source struct {
	sdk.UnimplementedSource

	config SourceConfig
	host   string
	port   int32

	replica *kafka.Replica
	handler kafka.RequestHandler
	server  *kafka.Server

	messages *internal.Fanin[kafka.Batch]

	cached []sdk.Record
}

type SourceConfig struct {
	Config
}

func NewSource() sdk.Source {
	// Create Source and wrap it in the default middleware.
	return sdk.SourceWithMiddleware(&Source{}, sdk.DefaultSourceMiddleware()...)
}

func (s *Source) Parameters() map[string]sdk.Parameter {
	return s.config.Parameters()
}

func (s *Source) Configure(ctx context.Context, cfg map[string]string) error {
	err := sdk.Util.ParseConfig(cfg, &s.config)
	if err != nil {
		return fmt.Errorf("invalid config: %w", err)
	}

	addrSplit := strings.Split(s.config.Addr, ":")
	if len(addrSplit) != 2 {
		return fmt.Errorf("invalid address: %s", s.config.Addr)
	}
	port, err := strconv.ParseInt(addrSplit[1], 10, 32)
	if err != nil {
		return fmt.Errorf("invalid port %s: %w", addrSplit[1], err)
	}

	s.host = addrSplit[0]
	s.port = int32(port)

	return nil
}

func (s *Source) Open(ctx context.Context, _ sdk.Position) error {
	logger := sdk.Logger(ctx)

	s.messages = internal.NewFanin[kafka.Batch]()

	s.replica = kafka.NewReplica(s.host, s.port)
	s.handler = kafka.NewSourceRequestHandler(s.replica)
	s.server = kafka.NewServer(s.handler, *logger)

	s.replica.OnChange(func() {
		s.messages.Reload(s.replica.Queues())
	})

	go func() {
		// TODO handle error
		_ = s.server.Run(ctx, s.config.Addr)
	}()

	return nil
}

func (s *Source) Read(ctx context.Context) (sdk.Record, error) {
	if len(s.cached) > 0 {
		rec := s.cached[0]
		s.cached = s.cached[1:]
		return rec, nil
	}

	batch, err := s.messages.Recv(ctx)
	if err != nil {
		return sdk.Record{}, err
	}

	s.cached = make([]sdk.Record, int(batch.NumRecords))
	recs := readRawRecords(int(batch.NumRecords), batch.Records)
	for i, rec := range recs {
		// TODO what if the rest of the records are never read? We can lose stuff here
		s.cached[i] = sdk.Util.Source.NewRecordCreate(
			[]byte(fmt.Sprintf("%s:%d:%d", batch.Topic, batch.Partition, batch.Offset)),
			sdk.Metadata{
				"kafkabroker.topic":     batch.Topic,
				"kafkabroker.partition": strconv.FormatInt(int64(batch.Partition), 10),
				"kafkabroker.offset":    strconv.FormatInt(batch.Offset+int64(rec.OffsetDelta), 10),
			},
			sdk.RawData(rec.Key),
			sdk.RawData(rec.Value),
		)
		for _, header := range rec.Headers {
			s.cached[i].Metadata[header.Key] = string(header.Value)
		}
	}

	rec := s.cached[0]
	s.cached = s.cached[1:]
	return rec, nil
}

func (s *Source) Ack(context.Context, sdk.Position) error { return nil }

// TODO
func (s *Source) Teardown(context.Context) error { return nil }

// readRawRecords reads n records from in and returns them, returning early if
// there were partial records.
func readRawRecords(n int, in []byte) []kmsg.Record {
	rs := make([]kmsg.Record, n)
	for i := 0; i < n; i++ {
		length, used := kbin.Varint(in)
		total := used + int(length)
		if used == 0 || length < 0 || len(in) < total {
			return rs[:i]
		}
		if err := (&rs[i]).ReadFrom(in[:total]); err != nil {
			return rs[:i]
		}
		in = in[total:]
	}
	return rs
}
