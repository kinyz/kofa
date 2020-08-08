package kofa

import (
	"time"
)

type Request interface {
	GetProducer() string
	GetData() []byte
	GetMessage() Message
	Call(alias, method string, data []byte, service ...string) error
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
}
