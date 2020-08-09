package kofa

import (
	"github.com/Shopify/sarama"
	"time"
)

type Request interface {
	GetProducer() string
	GetMsgId() uint64
	GetData() []byte
	GetKey() string
	GetMessage() Message
	Call(msgId uint64, key string, data []byte, topic ...string) error
}

type IKafkaRequest interface {
	CustomHandle(msg Message)
}

type Message interface {
	GetData() []byte
	GetTopic() string
	GetKey() string
	GetOffset() int64
	GetPart() int32
	GetTimestamp() time.Time
	GetBlockTimestamp() time.Time
	GetHeaders() []*sarama.RecordHeader
}
