package prehandle

import (
	"context"
	"fmt"
	"github.com/Shopify/sarama"
	"kofa"
	"kofa/message"
	"log"
	"sync"
	"time"
)

const NewOffset = -1
const OldOffset = -2

func Kafka(Addr []string, Offset int64) kofa.Message {
	k := &IKafka{Addr: Addr, Offset: Offset}
	err := k.NewSyncProducer()
	if err != nil {
		panic(err)
	}
	err = k.NewAsyncProducer()
	if err != nil {
		panic(err)
	}
	return k

}

type IKafka struct {
	Addr   []string
	Offset int64
	Group  string
	sarama.AsyncProducer
	sarama.SyncProducer
	kofa.Server
	closeFun func()
}

func (k *IKafka) Initialize(server kofa.Server) {
	k.Server = server
	k.closeFun = func() {}
}

func (k *IKafka) Send(msg message.Message, topic ...string) error {
	ser, err := k.ServiceManager().Get(msg.Header().GetMsgId())
	if err != nil {
		return err
	}

	if msg.Header().GetProducer() == "" {
		msg.Header().SetProducer(k.Server.Info().GetId())
	}
	msg.Header().SetTimesTamp(time.Now().UnixNano())
	head, _ := msg.Header().Encode()
	if len(topic) < 1 {
		k.Async(
			ser.GetTopic(),
			[]byte(msg.GetKey()),
			msg.GetData(),
			sarama.RecordHeader{Key: []byte("header"), Value: head})
		return nil
	}

	for _, v := range topic {
		k.Async(
			v,
			[]byte(msg.GetKey()),
			msg.GetData(),
			sarama.RecordHeader{Key: []byte("header"), Value: head})
	}
	return nil
}

func (k *IKafka) Close() {
	k.closeFun()
}

func (k *IKafka) Listen(group string, topic ...string) error {
	config := sarama.NewConfig()
	config.Version = sarama.V2_3_0_0
	config.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategyRange // 分区分配策略
	config.Consumer.Offsets.Initial = k.Offset                             // 未找到组消费位移的时候从哪边开始消费
	config.ChannelBufferSize = 2                                           // channel长度
	client, err := sarama.NewConsumerGroup(k.Addr, group, config)
	//client := r.IConsumer.GetConsumerGroup()
	if err != nil {
		log.Println("[消息监听组]: ", topic, " 启动失败", err)
		return err
	}

	consumer := &Consumer{Server: k, ready: make(chan bool)}
	ctx, cancel := context.WithCancel(context.Background())
	wg := &sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer func() {
			wg.Done()
		}()
		for {
			//log.Println(r.Topic)
			//log.Println("[消息监听组]: ", []string{"test_go"}, group ,addr,offset)

			if err := client.Consume(ctx, topic, consumer); err != nil {
				log.Fatalf("Error from consumer: %v", err)
			}
			// check if context was cancelled, signaling that the consumer should stop
			if ctx.Err() != nil {
				log.Println(ctx.Err())
				return
			}
			consumer.ready = make(chan bool)
		}
	}()
	<-consumer.ready
	log.Println("[消息监听组]: ", topic, " 已开启监听")
	k.closeFun = func() {
		cancel()
		wg.Wait()
		if err := client.Close(); err != nil {
			log.Println("Error closing client")
		}
		log.Println("[消息监听组]: ", topic, " 已关闭监听")
	}
	return nil
}

func (k *IKafka) NewSyncProducer() error {
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	config.Producer.Partitioner = sarama.NewRandomPartitioner
	config.Producer.Timeout = 5 * time.Second
	config.Version = sarama.V2_3_0_0
	sync, err := sarama.NewSyncProducer(k.Addr, config)
	if err != nil {
		//log.Println("[ERROR]:NewSyncProducer fail! err=" + err.Error())
		return err
	}
	k.SyncProducer = sync
	return nil
}
func (k *IKafka) NewAsyncProducer() error {
	config := sarama.NewConfig()
	config.Producer.Return.Successes = false
	config.Producer.Partitioner = sarama.NewRandomPartitioner
	config.Producer.Timeout = 5 * time.Second
	config.Version = sarama.V2_3_0_0
	async, err := sarama.NewAsyncProducer(k.Addr, config)
	if err != nil {
		//	log.Println("[ERROR]:NewAsyncProducer fail! err=" + err.Error())
		return err
	}
	k.AsyncProducer = async
	return nil
}

func (k *IKafka) Sync(topic string, key, data []byte, headers ...sarama.RecordHeader) (int32, int64, error) {
	//log.Println("sync",headers)
	msg := &sarama.ProducerMessage{
		Topic:     topic,
		Key:       sarama.ByteEncoder(key),
		Value:     sarama.ByteEncoder(data),
		Timestamp: time.Time{},
		Headers:   headers,
	}
	part, offset, err := k.SyncProducer.SendMessage(msg)
	if err != nil {
		fmt.Printf("send message(%s) err=%s \n", data, err)
		return 0, 0, err
	} else {

		return part, offset, err
	}
}

func (k *IKafka) Async(topic string, key, data []byte, headers ...sarama.RecordHeader) {
	async := k.AsyncProducer
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

// Consumer represents a Sarama consumer group consumer
type Consumer struct {
	ready chan bool
	kofa.Server
}

// Setup is run at the beginning of a new session, before ConsumeClaim
func (consumer *Consumer) Setup(sarama.ConsumerGroupSession) error {
	// Mark the consumer as ready
	close(consumer.ready)
	return nil
}

// Cleanup is run at the end of a session, once all ConsumeClaim goroutines have exited
func (consumer *Consumer) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

// ConsumeClaim must start a consumer loop of ConsumerGroupClaim's Messages().
func (consumer *Consumer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for msg := range claim.Messages() {

		if len(msg.Headers) > 0 {
			kMsg := message.NewMessage()
			for _, v := range msg.Headers {
				if string(v.Key) == "header" {
					err := kMsg.Header().Decode(v.Value)
					if err != nil {
						log.Println(err)
						session.MarkMessage(msg, "")
						continue
					}
				}
			}

			kMsg.SetKey(string(msg.Key))
			kMsg.SetData(msg.Value)
			ser, err := consumer.ServiceManager().Get(kMsg.Header().GetMsgId())
			if err != nil {
				log.Println("kafka msg error ", err)
			} else {
				_ = consumer.Server.Reflect().Call(
					ser.GetAlias(),
					ser.GetMethod(),
					kofa.NewRequest(consumer.Server, kMsg),
				)
			}
		}
		session.MarkMessage(msg, "")
	}

	return nil
}
