package main

import (
	"kofa/ikofa"
	"kofa/kofa"
	"log"
	"time"
)

const ServiceName2 = "Oauth"

func main() {
	k := ikofa.NewServer(ServiceName2, ikofa.NewOffset, []string{"49.22.22.22:2222"}, true)
	k.AddRouter(3000, "Oauth", &Oauth{})

	//k.CustomHandle(&Kafka{})
	go func() {
		time.Sleep(time.Second * 20)
		err := k.Call(2001, k.GetServerId(), []byte("hi kofa"))
		log.Println("发送成功")
		if err != nil {
			log.Println(err)
		}
		err = k.Call(2002, k.GetServerId(), []byte("hi kofa"))
		if err != nil {
			log.Println("2", err)
		}

		k.Send().Async("Kofa", []byte("test_key"), []byte("test_data"))

	}()
	k.Serve()

}

type Oauth struct {
}

func (o *Oauth) Account(request kofa.Request) {
	request.GetProducer()
}
