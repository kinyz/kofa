package main

import (
	"fmt"
	"github.com/Shopify/sarama"
	"kofa"
	"kofa/ikofa"
	"time"
)

const ServiceName = "Kofa"

func main() {

	k := kofa.NewServer(ServiceName, kofa.NewOffset, []string{"49.22.220.15:13900"}, true)
	k.AddRouter("User", &User{})

	k.CustomHandle(&Kafka{})
	go func() {
		time.Sleep(time.Second * 20)
		err := k.Call("User", "Login", []byte("hi kofa"))
		fmt.Println("发送成功")
		if err != nil {
			fmt.Println(err)
		}
		err = k.Call("User", "Login", []byte("hi kofa"), k.GetServerId())
		if err != nil {
			fmt.Println("2", err)
		}

		k.Send().Async(k.GetServerId(), []byte("test_key"), []byte("test_data"))

	}()
	k.Serve()

}

type User struct {
}

func (u *User) Login(request ikofa.IRequest) {

	fmt.Println(request.GetProducer())

}

func (u *User) Reg(request ikofa.IRequest) {

}

type Kafka struct {
}

func (k Kafka) CustomHandle(msg *sarama.ConsumerMessage) {
	fmt.Println(string(msg.Key), string(msg.Value))
}
