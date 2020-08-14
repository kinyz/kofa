package main

import (
	"kofa"
	"kofa/message"
	"kofa/prehandle"
	"log"
	"time"
)

func main() {
	s := kofa.New("Oauth", true, prehandle.Kafka([]string{"181.1.1.1:9092"}, prehandle.NewOffset))

	a := &Oauth{num: 0, time: make(map[uint64]int64)}
	s.AddRouter(3000, "Account", 5, a)

	go func() {
		time.Sleep(time.Second * 30)
		msg := message.NewMessage()
		msg.Header().SetMsgId(2003)
		msg.SetKey("im key")
		msg.SetData([]byte("im data"))
		msg.Property().Set("key1", []byte("data1"))
		msg.Property().Set("key2", []byte("data2"))

		//log.Println("sermap",s.ServiceManager().GetAll())

		log.Println("action send")

		a.now = time.Now().UnixNano()
		for i := 0; i < 5; i++ {
			//msg.Header().SetTimesTamp(time.Now().UnixNano())

			err := s.Send(msg)
			if err != nil {
				log.Println("send msg err:", err)
			}

		}

	}()

	s.Serve()
}

type Oauth struct {
	time map[uint64]int64
	num  uint64
	now  int64
}

func (u *Oauth) Account(request kofa.Request) {
	now := time.Now().UnixNano()
	ns := now - request.Message().Header().GetTimesTamp()
	log.Println("call back now:", now, "ns:", ns)

	log.Println("数据统计：", 1, "条")
	log.Println("开始时间:", u.now, "ns")
	log.Println("到达时间:", now, "ns")
	log.Println("共耗时:", now-u.now, "ns")
	log.Println("平均耗时:", (now-u.now)/1, "ns")

}
func (u *Oauth) Logout(request kofa.Request) {

}
