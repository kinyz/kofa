package ikofa

import (
	"context"
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/golang/protobuf/proto"
	"kofa/kofa"
	apis "kofa/pd"
	"log"
	"reflect"
	"sync"
)

type Router struct {
	obj   map[string]interface{}
	param map[string][]reflect.Value
	//methodMap map[string][]string
	kreq       kofa.IKafkaRequest
	c          bool
	serviceMap map[uint64]*apis.Service

	kofa *Server
}

func NewRouter(kofa *Server) *Router {
	return &Router{
		obj:   make(map[string]interface{}),
		param: make(map[string][]reflect.Value),
		//	methodMap: make(map[string][]string),
		serviceMap: make(map[uint64]*apis.Service),
		kofa:       kofa,
	}
}

// 注册组监听
func (r *Router) StartListen(addr, topic []string, group string, offset int64) func() {
	config := sarama.NewConfig()
	config.Version = sarama.V2_3_0_0
	config.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategyRange // 分区分配策略
	config.Consumer.Offsets.Initial = offset                               // 未找到组消费位移的时候从哪边开始消费
	config.ChannelBufferSize = 2                                           // channel长度
	client, err := sarama.NewConsumerGroup(addr, group, config)
	//client := r.IConsumer.GetConsumerGroup()
	if err != nil {
		fmt.Println("[消息监听组]: ", topic, " 启动失败", err)
		return nil
	}

	consumer := Consumer{router: r, ready: make(chan bool)}
	ctx, cancel := context.WithCancel(context.Background())
	wg := &sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer func() {
			wg.Done()
		}()
		for {
			//fmt.Println(r.Topic)
			//fmt.Println("[消息监听组]: ", []string{"test_go"}, group ,addr,offset)

			if err := client.Consume(ctx, topic, &consumer); err != nil {
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
	fmt.Println("[消息监听组]: ", topic, " 已开启监听")
	return func() {
		cancel()
		wg.Wait()
		if err := client.Close(); err != nil {
			fmt.Println("Error closing client")
		}
		fmt.Println("[消息监听组]: ", topic, " 已关闭监听")
	}
}

func (r *Router) AddRouter(msgId uint64, serverId, topic, alias string, obj interface{}, param ...interface{}) {
	r.param[alias] = make([]reflect.Value, len(param)+1)

	for i, _ := range param {
		r.param[alias][i+1] = reflect.ValueOf(param[i])
		//name:=reflect.TypeOf(param[i])
		//fmt.Println(name.Name())
	}
	ref := reflect.TypeOf(obj)
	fmt.Println("------add router", alias, "--------")
	r.obj[alias] = obj
	for i := 1; i < ref.NumMethod()+1; i++ {
		//path:=strings.ToLower(name+"."+ref.Method(i).Name)//转换路径为全小写
		//path := alias + "." + ref.Method(i).Name
		s := &apis.Service{MsgId: msgId + uint64(i), ServerId: serverId, Topic: topic, Alias: alias, Method: ref.Method(i - 1).Name}
		r.serviceMap[msgId+uint64(i)] = s
		r.kofa.discover.Add(s)
		//fmt.Println("add msg ",msgId+uint64(i))
	}
}
func (r *Router) CustomHandle(kafka kofa.IKafkaRequest) {
	r.kreq = kafka
	r.c = true
}
func (r *Router) GetServiceMap() map[uint64]*apis.Service {
	return r.serviceMap
}

// Consumer represents a Sarama consumer group consumer
type Consumer struct {
	ready  chan bool
	router *Router
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
			call := &apis.Call{}
			err := proto.UnmarshalMerge(msg.Headers[0].Key, call)
			if err != nil {
				if consumer.router.c {
					consumer.router.kreq.CustomHandle(&KafkaMessage{
						msg: msg,
					})
				}
			} else {
				service := consumer.router.serviceMap[call.MsgId]
				if consumer.router.obj[service.Alias] == nil {
					session.MarkMessage(msg, "")
					break
				}
				req := &KRequest{
					msg:  msg,
					call: call,
					kofa: consumer.router.kofa,
				}
				consumer.router.param[service.Alias][0] = reflect.ValueOf(req)
				if consumer.router.obj[service.Alias] != nil {
					reflect.ValueOf(consumer.router.obj[service.Alias]).MethodByName(service.Method).Call(consumer.router.param[service.Alias])
				}

			}
		} else {
			if consumer.router.c {
				consumer.router.kreq.CustomHandle(&KafkaMessage{
					msg: msg,
				})
			}
		}
		session.MarkMessage(msg, "")

	}
	return nil
}
