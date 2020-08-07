package kface

import (
	"github.com/Shopify/sarama"
	"time"
)

type IRequest interface {
	GetTopic() string
	GetProducer() string
	GetTimestamp() time.Time
	GetOffset() int64
	GetPartition() int32
	GetData() []byte
	GetRawMsg() *sarama.ConsumerMessage
}
