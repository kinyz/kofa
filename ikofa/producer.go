package ikofa

import (
	"fmt"
	"github.com/Shopify/sarama"
	"log"
	"time"
)

type Producer struct {
	sarama.AsyncProducer
	sarama.SyncProducer
}

func NewIProducer(addr []string) *Producer {
	p := &Producer{}
	err := p.NewAsyncProducer(addr)
	if err != nil {
		log.Println("kafka producer conn err :", err)
		return nil
	}
	err = p.NewSyncProducer(addr)
	if err != nil {
		log.Println("kafka producer conn err :", err)
		return nil
	}
	return p
}

func (p *Producer) NewSyncProducer(addr []string) error {
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	config.Producer.Partitioner = sarama.NewRandomPartitioner
	config.Producer.Timeout = 5 * time.Second
	config.Version = sarama.V2_3_0_0
	sync, err := sarama.NewSyncProducer(addr, config)
	if err != nil {
		//log.Println("[ERROR]:NewSyncProducer fail! err=" + err.Error())
		return err
	}
	p.SyncProducer = sync
	return nil
}
func (p *Producer) NewAsyncProducer(addr []string) error {
	config := sarama.NewConfig()
	config.Producer.Return.Successes = false
	config.Producer.Partitioner = sarama.NewRandomPartitioner
	config.Producer.Timeout = 5 * time.Second
	config.Version = sarama.V2_3_0_0
	async, err := sarama.NewAsyncProducer(addr, config)
	if err != nil {
		//	log.Println("[ERROR]:NewAsyncProducer fail! err=" + err.Error())
		return err
	}
	p.AsyncProducer = async
	return nil
}

// AsyncSendMsg 同步生产者
// 返回 part, offset, err
func (p *Producer) Sync(topic string, key, data []byte, headers ...sarama.RecordHeader) (int32, int64, error) {
	//log.Println("sync",headers)

	msg := &sarama.ProducerMessage{
		Topic:     topic,
		Key:       sarama.ByteEncoder(key),
		Value:     sarama.ByteEncoder(data),
		Timestamp: time.Time{},
		Headers:   headers,
	}
	part, offset, err := p.SyncProducer.SendMessage(msg)
	if err != nil {
		fmt.Printf("send message(%s) err=%s \n", data, err)
		return 0, 0, err
	} else {

		return part, offset, err
	}
}

// AsyncSendMsg 异步生产者
// 并发量大时，必须采用这种方式
func (p *Producer) Async(topic string, key, data []byte, headers ...sarama.RecordHeader) {
	async := p.GetAsyncProducer()
	//log.Println("async",headers)
	go func(as sarama.AsyncProducer) {
		errors := as.Errors()
		success := as.Successes()
		for {
			select {
			case err := <-errors:
				if err != nil {
					log.Println(err)
				}
			case <-success:
			}
		}
	}(async)

	msg := &sarama.ProducerMessage{
		Topic:     topic,
		Key:       sarama.StringEncoder(key),
		Value:     sarama.ByteEncoder(data),
		Timestamp: time.Time{},
		Headers:   headers,
	}
	async.Input() <- msg
}

func (p *Producer) GetSyncProducer() sarama.SyncProducer {
	return p.SyncProducer
}
func (p *Producer) GetAsyncProducer() sarama.AsyncProducer {
	return p.AsyncProducer
}
func (p *Producer) CloseAsyncProducer() {
	if err := p.AsyncProducer.Close(); err != nil {
		log.Println("CloseAsyncProducer Fail err=", err)
	}

}
func (p *Producer) CloseSyncProducer() {
	if err := p.SyncProducer.Close(); err != nil {
		log.Println("CloseSyncProducer Fail err=", err)
	}
}
