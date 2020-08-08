package ikofa

import (
	"github.com/Shopify/sarama"
	"kofa/kofa"
	"time"
)

type KRequest struct {
	msg      *sarama.ConsumerMessage
	producer string
	kofa     kofa.IServer
}

func (r *KRequest) GetProducer() string {
	return r.producer
}

func (r *KRequest) GetData() []byte {
	return r.msg.Value
}
func (r *KRequest) GetMessage() kofa.Message {
	return &KafkaMessage{
		msg: r.msg,
	}
}
func (r *KRequest) Call(alias, method string, data []byte, service ...string) error {
	return r.kofa.Call(alias, method, data, service...)
}

type KafkaMessage struct {
	msg *sarama.ConsumerMessage
}

func (k *KafkaMessage) GetData() []byte {
	return k.msg.Value
}
func (k *KafkaMessage) GetTopic() string {
	return k.msg.Topic
}
func (k *KafkaMessage) GetKey() string {
	return string(k.msg.Key)
}
func (k *KafkaMessage) GetOffset() int64 {
	return k.msg.Offset
}

func (k *KafkaMessage) GetPart() int32 {
	return k.msg.Partition
}

func (k *KafkaMessage) GetTimestamp() time.Time {
	return k.msg.Timestamp
}

func (k *KafkaMessage) GetBlockTimestamp() time.Time {
	return k.msg.BlockTimestamp
}
