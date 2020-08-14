package main

import (
	"kofa"
	"kofa/prehandle"
	"log"
	"time"
)

func main() {

	s := kofa.New("Test", true, prehandle.Kafka([]string{"10.43.123.172:9092"}, prehandle.NewOffset))

	a := &Account{}
	s.AddRouter(2000, "Account", 5, a)

	s.Serve()
}

type Account struct {
}

func (u *Account) Oauth(request kofa.Request) {

	log.Println("call logout oauth:", time.Now().UnixNano()-request.Message().Header().GetTimesTamp())

}
func (u *Account) Logout(request kofa.Request) {

	log.Println("call logout us:", time.Now().UnixNano()-request.Message().Header().GetTimesTamp())
	request.Message().Header().SetMsgId(3001)
	request.Send(request.Message())

	//request.Send(request.Message())
	//log.Println(time.Now().UnixNano()/ 1e6)

}
func (u *Account) Add(request kofa.Request, kofa kofa.Server) {
	log.Println("call logout add:", time.Now().UnixNano()-request.Message().Header().GetTimesTamp())

}

func (u *Account) Check(request kofa.Request, kofa kofa.Server) {
	log.Println("call logout check:", time.Now().UnixNano()-request.Message().Header().GetTimesTamp())

}
