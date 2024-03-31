package kafka

import (
	"context"
	"sync"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"
)

type Batch struct {
	kmsg.RecordBatch
	Partition int32
	Topic     string
	Offset    int64
}

type TopicPartition struct {
	Topic     string
	Partition int32
	Queue     chan Batch

	Offset int64
}

type Topic struct {
	TopicID    [16]byte
	Topic      string
	Partitions []*TopicPartition
}

type Replica struct {
	Host string
	Port int32

	// TODO topics are pointers, we should deep copy them whenever we return them
	Topics []*Topic

	onChange func()
	m        sync.RWMutex
}

func NewReplica(Host string, Port int32) *Replica {
	return &Replica{
		Host:     Host,
		Port:     Port,
		Topics:   make([]*Topic, 0),
		onChange: func() {},
	}
}

func (r *Replica) OnChange(f func()) {
	r.m.Lock()
	defer r.m.Unlock()

	if r.onChange == nil {
		r.onChange = f
		return
	}
	old := r.onChange
	r.onChange = func() {
		old()
		f()
	}
}

func (r *Replica) Queues() []<-chan Batch {
	r.m.RLock()
	defer r.m.RUnlock()

	var queues []<-chan Batch
	for _, t := range r.Topics {
		for _, tp := range t.Partitions {
			queues = append(queues, tp.Queue)
		}
	}
	return queues
}

func (r *Replica) Produce(ctx context.Context, topic string, partition int32, batch kmsg.RecordBatch) (int64, *kerr.Error) {
	tp, err := r.GetTopicPartition(topic, partition)
	if err != nil {
		return -1, err
	}

	// TODO we don't have a lock, is that fine?
	select {
	case tp.Queue <- Batch{
		RecordBatch: batch,
		Partition:   partition,
		Topic:       topic,
		Offset:      tp.Offset,
	}:
		baseOffset := tp.Offset
		tp.Offset += int64(batch.NumRecords)
		return baseOffset, nil
	case <-ctx.Done():
		return -1, kerr.RequestTimedOut
	}
}

func (r *Replica) CreateTopic(topic string, topicID [16]byte, numPartitions int32) (*Topic, *kerr.Error) {
	r.m.Lock()
	defer r.m.Unlock()

	_, _, err := r.getTopic(topic)
	if err == nil {
		return nil, kerr.TopicAlreadyExists
	}

	if numPartitions <= 0 {
		numPartitions = 1
	}

	t := &Topic{
		Topic:      topic,
		TopicID:    topicID,
		Partitions: make([]*TopicPartition, numPartitions),
	}
	for i := range t.Partitions {
		t.Partitions[i] = &TopicPartition{
			Topic:     topic,
			Partition: int32(i),
			Queue:     make(chan Batch),
		}
	}
	r.Topics = append(r.Topics, t)

	r.notifyChange()
	return t, nil
}

func (r *Replica) DeleteTopic(topic string) (*Topic, *kerr.Error) {
	r.m.Lock()
	defer r.m.Unlock()

	i, t, err := r.getTopic(topic)
	if err != nil {
		return nil, err
	}

	r.Topics = append(r.Topics[:i], r.Topics[i+1:]...)

	r.notifyChange()
	return t, nil
}

func (r *Replica) GetTopics() []*Topic {
	r.m.RLock()
	defer r.m.RUnlock()
	return r.Topics
}

func (r *Replica) GetTopic(topic string) (*Topic, *kerr.Error) {
	r.m.RLock()
	defer r.m.RUnlock()
	_, t, err := r.getTopic(topic)
	return t, err
}

func (r *Replica) GetTopicPartition(topic string, partition int32) (*TopicPartition, *kerr.Error) {
	r.m.RLock()
	defer r.m.RUnlock()
	_, t, err := r.getTopic(topic)
	if err != nil {
		return nil, err
	}
	for _, tp := range t.Partitions {
		if tp.Partition == partition {
			return tp, nil
		}
	}
	return nil, kerr.UnknownTopicOrPartition
}

func (r *Replica) getTopic(topic string) (int, *Topic, *kerr.Error) {
	for i, t := range r.Topics {
		if t.Topic == topic {
			return i, t, nil
		}
	}
	return -1, nil, kerr.UnknownTopicOrPartition
}

func (r *Replica) notifyChange() {
	if r.onChange == nil {
		return
	}
	go r.onChange()
}
