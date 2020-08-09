package main

import (
	"fmt"
	"kofa/ikofa"
	"kofa/kofa"
)

const ServiceName = "Kofa"

func main() {

	k := ikofa.NewServer(ServiceName, ikofa.NewOffset, []string{"9.123.160.15:1302"}, true)
	k.AddRouter(2000, "User", &User{})

	k.CustomHandle(&Kafka{})

	k.Serve()

}

type User struct {
}

func (u *User) Login(request kofa.Request) {

	fmt.Println(request.GetProducer())

}

func (u *User) Reg(request kofa.Request) {
	fmt.Println(request.GetMsgId())

}

type Kafka struct {
}

func (k Kafka) CustomHandle(msg kofa.Message) {
	fmt.Println(msg.GetTopic(), msg.GetTopic())
}
