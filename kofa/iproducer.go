package kofa

import "github.com/Shopify/sarama"

type ISend interface {
	Async(topic string, key, data []byte, headers ...sarama.RecordHeader)
	Sync(topic string, key, data []byte, headers ...sarama.RecordHeader) (int32, int64, error)
}
