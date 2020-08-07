package core

import (
	"github.com/Shopify/sarama"
	"time"
)

type Request struct {
	msg      *sarama.ConsumerMessage
	producer string
}

func (r *Request) GetTopic() string {
	return r.msg.Topic
}
func (r *Request) GetProducer() string {
	return r.producer
}

func (r *Request) GetTimestamp() time.Time {
	return r.msg.Timestamp
}
func (r *Request) GetOffset() int64 {
	return r.msg.Offset
}

func (r *Request) GetPartition() int32 {
	return r.msg.Partition
}

func (r *Request) GetData() []byte {
	return r.msg.Value
}
func (r *Request) GetRawMsg() *sarama.ConsumerMessage {
	return r.msg
}
