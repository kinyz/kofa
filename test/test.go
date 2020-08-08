package main

import (
	"fmt"
	"github.com/Shopify/sarama"
	"kofa"
	"kofa/ikofa"
)

const ServiceName = "Kofa"

func main() {

	k := kofa.NewServer(ServiceName, kofa.NewOffset, []string{"31.21.160.15:13900"}, true)
	k.AddRouter("User", &User{})

	k.CustomHandle(&Kafka{})

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
