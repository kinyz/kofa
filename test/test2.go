package main

import (
	"fmt"
	"kofa/ikofa"
	"kofa/kofa"
	"time"
)

const ServiceName2 = "Oauth"

func main() {
	k := ikofa.NewServer(ServiceName2, ikofa.NewOffset, []string{"31.21.160.15:13900"}, true)
	k.AddRouter("Oauth", &Oauth{})

	//k.CustomHandle(&Kafka{})
	go func() {
		time.Sleep(time.Second * 20)
		err := k.Call("User", "Login", []byte("hi kofa"))
		fmt.Println("发送成功")
		if err != nil {
			fmt.Println(err)
		}
		err = k.Call("User", "Login", []byte("hi kofa"))
		if err != nil {
			fmt.Println("2", err)
		}

		k.Send().Async("Kofa", []byte("test_key"), []byte("test_data"))

	}()
	k.Serve()

}

type Oauth struct {
}

func (o *Oauth) Account(request kofa.Request) {

}
