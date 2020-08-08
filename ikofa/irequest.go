package ikofa

import (
	"github.com/Shopify/sarama"
)

type IRequest interface {
	GetProducer() string
	GetData() []byte
	GetMessage() *sarama.ConsumerMessage
	Call(alias, method string, data []byte, service ...string) error
}

type IKafkaRequest interface {
	CustomHandle(msg *sarama.ConsumerMessage)
}
