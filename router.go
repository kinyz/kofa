package kofa

import (
	"context"
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/golang/protobuf/proto"
	"kofa/ikofa"
	apis "kofa/pd"
	"log"
	"reflect"
	"sync"
)

type Router struct {
	obj   map[string]interface{}
	param map[string][]reflect.Value
	//methodMap map[string][]string
	kreq       ikofa.IKafkaRequest
	c          bool
	serviceMap map[string][]*apis.Service
	kofa       *Server
}

func NewRouter(kofa *Server) *Router {
	return &Router{
		obj:   make(map[string]interface{}),
		param: make(map[string][]reflect.Value),
		//	methodMap: make(map[string][]string),
		serviceMap: make(map[string][]*apis.Service),
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

func (r *Router) AddRouter(serverId, topic, alias string, obj interface{}, param ...interface{}) {
	r.param[alias] = make([]reflect.Value, len(param)+1)

	for i, _ := range param {
		r.param[alias][i+1] = reflect.ValueOf(param[i])
		//name:=reflect.TypeOf(param[i])
		//fmt.Println(name.Name())
	}
	ref := reflect.TypeOf(obj)
	fmt.Println("------add router", alias, "--------")
	for i := 0; i < ref.NumMethod(); i++ {
		//path:=strings.ToLower(name+"."+ref.Method(i).Name)//转换路径为全小写
		path := alias + "." + ref.Method(i).Name
		s := &apis.Service{ServerId: serverId, Topic: topic, Alias: alias, Method: ref.Method(i).Name}
		r.serviceMap[alias] = append(r.serviceMap[alias], s)
		r.obj[path] = obj
		r.kofa.discover.Add(s)
		//r.methodMap[alias] = append(r.methodMap[alias], path)
		fmt.Println("add method: ", path)
	}
}
func (r *Router) CustomHandle(kafka ikofa.IKafkaRequest) {
	r.kreq = kafka
	r.c = true
}
func (r *Router) GetServiceMap() map[string][]*apis.Service {
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
		call := &apis.Call{}
		err := proto.UnmarshalMerge(msg.Key, call)

		if err != nil {
			if consumer.router.c {
				consumer.router.kreq.CustomHandle(msg)
			}
		} else {
			path := call.Alias + "." + call.Method
			if consumer.router.obj[path] == nil {
				fmt.Println("not find method :", path)
				session.MarkMessage(msg, "")
				break
			}
			req := &Request{
				msg:      msg,
				producer: call.Producer,
				kofa:     consumer.router.kofa,
			}
			consumer.router.param[call.Alias][0] = reflect.ValueOf(req)
			if consumer.router.obj[path] != nil {
				reflect.ValueOf(consumer.router.obj[path]).MethodByName(call.Method).Call(consumer.router.param[call.Alias])
			}

			session.MarkMessage(msg, "")
		}

	}
	return nil
}
