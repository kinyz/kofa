package kofa

import (
	"errors"
	"fmt"
	"github.com/golang/protobuf/proto"
	"github.com/satori/go.uuid"
	"kofa/ikofa"
	apis "kofa/pd"
	"os"
	"os/signal"
	"syscall"
)

const NewOffset = -1
const OldOffset = -2

type Server struct {
	addr                   []string
	router                 *Router
	serverId, topic, group string
	discover               *Discovery
	send                   ikofa.ISend
	closeFun               func()
	offset                 int64
	done                   chan bool
}

func NewServer(serviceName string, offset int64, kafkaAddr []string, group bool) ikofa.IServer {
	s := &Server{
		topic:    serviceName,
		offset:   offset,
		addr:     kafkaAddr,
		send:     NewIProducer(kafkaAddr),
		serverId: serviceName + "-" + uuid.NewV4().String()}
	s.group = s.GetServerId()
	if group {
		s.group = s.serverId
	}
	s.router = NewRouter(s)
	s.discover = NewIDiscovery(s)
	s.discover.Start(s.addr, true)
	return s
}

func (s *Server) AddRouter(alias string, obj interface{}, param ...interface{}) {
	s.router.AddRouter(s.serverId, s.topic, alias, obj, param...)
}
func (s *Server) CustomHandle(kafka ikofa.IKafkaRequest) {
	s.router.CustomHandle(kafka)
}
func (s *Server) Serve() {
	s.discover.Register(ServiceDiscoveryTopic)
	s.discover.CheckAllService()
	s.closeFun = s.router.StartListen(s.addr, []string{s.topic}, s.group, s.offset)
	sigs := make(chan os.Signal, 1)
	s.done = make(chan bool, 1)

	signal.Notify(sigs, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)
	go func() {
		<-sigs

		//	fmt.Println(sig)
		s.done <- true
	}()

	fmt.Println("kofa server running ServerId:", s.GetServerId(), "Topic:", s.topic)
	<-s.done

	s.discover.Logout()
	s.discover.Close()
	s.closeFun()
}

func (s *Server) Close() {
	s.done <- true

}

func (s *Server) Call(alias, method string, data []byte, service ...string) error {
	if s.discover.GetTopicByMethod(alias, method) == "" {
		return errors.New("not find method:" + alias + "." + method)
	}
	call, _ := proto.Marshal(&apis.Call{Alias: alias, Method: method, Producer: s.serverId})
	if len(service) <= 0 {
		s.send.Async(s.discover.GetTopicByMethod(alias, method), call, data)
		return nil
	}
	for _, v := range service {
		s.send.Async(v, call, data)
	}
	return nil
}

func (s *Server) Send() ikofa.ISend {
	return s.send
}
func (s *Server) GetServerId() string {
	return s.serverId
}
