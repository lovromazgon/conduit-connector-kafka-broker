package kafkabroker

import (
	"github.com/rs/zerolog"
	"github.com/travisjeffery/jocko/jocko/structs"
	"github.com/twmb/franz-go/pkg/kbin"
	"github.com/twmb/franz-go/pkg/kmsg"
	"io"
)

type Message struct {
	kmsg.Record
	Partition int32
	Topic     string
	Offset    int64
}

type TeeCommitLog struct {
	partition int32
	topic     string
	log       structs.CommitLog
	out       chan Message

	logger zerolog.Logger
}

func NewTeeCommitLog(log structs.CommitLog, p structs.Partition, logger *zerolog.Logger) *TeeCommitLog {
	return &TeeCommitLog{
		partition: p.Partition,
		topic:     p.Topic,
		log:       log,
		out:       make(chan Message),

		logger: logger.With().
			Str("topic", p.Topic).
			Int32("partition", p.Partition).Logger(),
	}
}

func (c TeeCommitLog) Delete() error { return c.log.Delete() }
func (c TeeCommitLog) NewReader(offset int64, maxBytes int32) (io.Reader, error) {
	return c.log.NewReader(offset, maxBytes)
}
func (c TeeCommitLog) Truncate(i int64) error { return c.log.Truncate(i) }
func (c TeeCommitLog) NewestOffset() int64    { return c.log.NewestOffset() }
func (c TeeCommitLog) OldestOffset() int64    { return c.log.OldestOffset() }
func (c TeeCommitLog) Append(bytes []byte) (int64, error) {
	c.logger.Trace().Msg("appending message")
	offset, err := c.log.Append(bytes)
	if err != nil {
		c.logger.Err(err).Msg("appending failed")
		return offset, err
	}

	var batch kmsg.RecordBatch
	err = batch.ReadFrom(bytes)
	if err != nil {
		panic(err)
	}

	c.logger.Trace().Int32("count", batch.NumRecords).Msg("appending successful, producing records")
	recs := readRawRecords(int(batch.NumRecords), batch.Records)
	for i, rec := range recs {
		// TODO what if the message is never read? Handle graceful shutdown.
		c.out <- Message{
			Record:    rec,
			Partition: c.partition,
			Topic:     c.topic,
			Offset:    offset + int64(rec.OffsetDelta),
		}
		c.logger.Trace().Int64("offset", offset+int64(i)).Msg("message produced")
	}
	c.logger.Trace().Msg("all messages produced")
	return offset, nil
}
func (c TeeCommitLog) Messages() <-chan Message {
	return c.out
}

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
