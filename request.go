package kofa

import (
	"github.com/Shopify/sarama"
	"kofa/ikofa"
)

type Request struct {
	msg      *sarama.ConsumerMessage
	producer string
	kofa     ikofa.IServer
}

func (r *Request) GetProducer() string {
	return r.producer
}

func (r *Request) GetData() []byte {
	return r.msg.Value
}
func (r *Request) GetMessage() *sarama.ConsumerMessage {
	return r.msg
}
func (r *Request) Call(alias, method string, data []byte, service ...string) error {
	return r.kofa.Call(alias, method, data, service...)
}
