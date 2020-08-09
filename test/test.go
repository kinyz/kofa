package main

import (
	"kofa/ikofa"
	"kofa/kofa"
	"log"
)

const ServiceName = "Kofa"

func main() {

	k := ikofa.NewServer(ServiceName, ikofa.NewOffset, []string{"49.22.22.22:2222"}, true)
	k.AddRouter(2000, "User", &User{})

	k.CustomHandle(&Kafka{})

	k.Serve()

}

type User struct {
}

func (u *User) Login(request kofa.Request) {

	log.Println(request.GetProducer())

}

func (u *User) Reg(request kofa.Request) {
	log.Println(request.GetMsgId())

}

type Kafka struct {
}

func (k Kafka) CustomHandle(msg kofa.Message) {
	log.Println(msg.GetTopic(), msg.GetTopic())
}
