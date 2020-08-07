package core

import (
	"context"
	"fmt"
	"github.com/Shopify/sarama"
	"log"
	"reflect"
	"strings"
	"sync"
)

type Router struct {
	ready     chan bool
	obj       map[string]interface{}
	param     map[string][]reflect.Value
	methodMap map[string][]string
}

func NewRouter() *Router {
	return &Router{ready: make(chan bool),
		obj:       make(map[string]interface{}),
		param:     make(map[string][]reflect.Value),
		methodMap: make(map[string][]string),
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

	r.ready = make(chan bool)
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

			if err := client.Consume(ctx, topic, r); err != nil {
				log.Fatalf("Error from consumer: %v", err)
			}
			// check if context was cancelled, signaling that the consumer should stop
			if ctx.Err() != nil {
				log.Println(ctx.Err())
				return
			}
			r.ready = make(chan bool)
		}
	}()
	<-r.ready
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

// Setup is run at the beginning of a new session, before ConsumeClaim
func (r *Router) Setup(sarama.ConsumerGroupSession) error {

	// Mark the consumer as ready

	close(r.ready)
	return nil
}

// Cleanup is run at the end of a session, once all ConsumeClaim goroutines have exited
func (r *Router) Cleanup(sarama.ConsumerGroupSession) error {

	return nil
}

// ConsumeClaim must start a consumer loop of ConsumerGroupClaim's Messages().
func (r *Router) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for msg := range claim.Messages() {
		//	fmt.Println("收到消息",msg)

		kov := strings.Split(string(msg.Key), ".")
		path := kov[1] + "." + kov[2]

		if r.obj[path] == nil {
			fmt.Println("not find obj :", string(msg.Key))
			session.MarkMessage(msg, "")
			break
		}
		req := &Request{
			msg:      msg,
			producer: kov[0],
		}

		r.param[kov[1]][0] = reflect.ValueOf(req)
		if r.obj[path] != nil {
			reflect.ValueOf(r.obj[path]).MethodByName(kov[2]).Call(r.param[kov[1]])

		}

		session.MarkMessage(msg, "")

	}
	return nil
}

func (r *Router) AddRouter(name string, obj interface{}, param ...interface{}) {
	r.param[name] = make([]reflect.Value, len(param)+1)
	for i, _ := range param {
		r.param[name][i+1] = reflect.ValueOf(param[i])
	}
	ref := reflect.TypeOf(obj)
	fmt.Println("------add router", name, "--------")
	for i := 0; i < ref.NumMethod(); i++ {
		//path:=strings.ToLower(name+"."+ref.Method(i).Name)//转换路径为全小写
		path := name + "." + ref.Method(i).Name
		r.obj[path] = obj
		r.methodMap[name] = append(r.methodMap[name], path)
		fmt.Println("add method: ", path)
	}
}

func (r *Router) GetMethodMap() map[string][]string {
	return r.methodMap
}
