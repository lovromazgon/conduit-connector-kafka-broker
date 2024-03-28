package kafkabroker

import (
	"github.com/travisjeffery/jocko/commitlog"
	"github.com/travisjeffery/jocko/jocko/structs"
	"io"
)

type Message struct {
	commitlog.Message
	Partition int32
	Topic     string
	Offset    int64
}

type TeeCommitLog struct {
	partition int32
	topic     string
	log       structs.CommitLog
	out       chan Message
}

func NewTeeCommitLog(log structs.CommitLog, p structs.Partition) *TeeCommitLog {
	return &TeeCommitLog{
		partition: p.Partition,
		topic:     p.Topic,
		log:       log,
		out:       make(chan Message),
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
	offset, err := c.log.Append(bytes)
	if err != nil {
		return offset, err
	}

	for i, msg := range commitlog.MessageSet(bytes).Messages() {
		// TODO what if the message is never read? Handle graceful shutdown.
		c.out <- Message{
			Message:   msg,
			Partition: c.partition,
			Topic:     c.topic,
			Offset:    offset + int64(i),
		}
	}
	return offset, nil
}
func (c TeeCommitLog) Messages() <-chan Message {
	return c.out
}
