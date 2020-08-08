package main

import (
	"fmt"
	"kofa/ikofa"
	"kofa/kofa"
)

const ServiceName = "Kofa"

func main() {

	k := ikofa.NewServer(ServiceName, ikofa.NewOffset, []string{"31.21.160.15:13900"}, true)
	k.AddRouter("User", &User{})

	k.CustomHandle(&Kafka{})

	k.Serve()

}

type User struct {
}

func (u *User) Login(request kofa.Request) {

	fmt.Println(request.GetProducer())

}

func (u *User) Reg(request kofa.Request) {
}

type Kafka struct {
}

func (k Kafka) CustomHandle(msg kofa.Message) {
	fmt.Println(msg.GetTopic(), msg.GetTopic())
}
