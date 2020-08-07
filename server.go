package kofa

import (
	"errors"
	"fmt"
	"github.com/satori/go.uuid"
	"kofa/core"
	"kofa/kface"
	"os"
	"os/signal"
	"syscall"
	"time"
)

const NewOffset = -1
const OldOffset = -2

type Server struct {
	addr            []string
	router          *core.Router
	serverId, topic string
	discover        *core.Discovery
	send            kface.ISend
}

func NewServer(topic, name string, kafkaAddr []string) kface.IServer {
	s := &Server{topic: topic, addr: kafkaAddr, router: core.NewRouter(), send: core.NewIProducer(kafkaAddr), serverId: name + uuid.NewV4().String()}
	s.discover = core.NewIDiscovery(s.router, s.send, s.serverId)
	return s
}

func (s *Server) AddRouter(name string, obj interface{}, param ...interface{}) {
	s.router.AddRouter(name, obj, param...)
}
func (s *Server) ListenRouter(group string, offset int64, topic ...string) {

	topic = append(topic, s.topic)

	s.router.StartListen(s.addr, topic, group, offset)

	s.discover.Start(s.addr, true, s.serverId)
	//fmt.Println(topic)

}
func (s *Server) Start() {
	s.discover.Register(s.GetServerTopic(), s.serverId, core.ServiceDiscoveryTopic)
	s.discover.CheckAllService(s.serverId)
	fmt.Println("kofa server running ServerId:", s.GetServerId(), "Topic:", s.topic)

}

func (s *Server) Serve() {
	s.Start()
	sigs := make(chan os.Signal, 1)
	done := make(chan bool, 1)

	signal.Notify(sigs, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)
	go func() {
		<-sigs

		//	fmt.Println(sig)
		done <- true
	}()

	//fmt.Println("awaiting signal")

	<-done
	s.Close()
}

func (s *Server) Close() {
	s.discover.Logout()
	time.Sleep(time.Second * 5)
}

func (s *Server) Call(producer, method string, data []byte) error {
	//fmt.Println(s.discover.GetTopicByMethod(method))
	if s.discover.GetTopicByMethod(method) != "" {
		s.send.Async(s.discover.GetTopicByMethod(method), producer+"."+method, data)
		return nil
	}
	return errors.New("not find method:" + method)
}
func (s *Server) GetServerTopic() string {
	return s.topic
}
func (s *Server) Send() kface.ISend {
	return s.send
}
func (s *Server) GetServerId() string {
	return s.serverId
}

func (s *Server) GetTopicByMethod(method string) string {
	return s.GetTopicByMethod(method)
}
func (s *Server) GetMethodByTopic(topic string) []string {
	return s.GetMethodByTopic(topic)
}
