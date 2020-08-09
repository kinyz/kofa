package ikofa

import (
	"errors"
	"github.com/Shopify/sarama"
	"github.com/golang/protobuf/proto"
	"github.com/satori/go.uuid"
	"kofa/kofa"
	apis "kofa/pd"
	"log"
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
	send                   kofa.ISend
	closeFun               func()
	offset                 int64
	done                   chan bool
}

func NewServer(serviceName string, offset int64, kafkaAddr []string, group bool) kofa.IServer {
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

func (s *Server) AddRouter(msgId uint64, alias string, obj interface{}, param ...interface{}) {
	if msgId < 2000 {
		log.Println(alias + " Failed to add. MsgId cannot be less than 2000")
		panic(alias + " Failed to add. MsgId cannot be less than 2000")
		return
	}

	s.router.AddRouter(msgId, s.serverId, s.topic, alias, obj, param...)

}
func (s *Server) CustomHandle(kafka kofa.IKafkaRequest) {
	s.router.CustomHandle(kafka)
}
func (s *Server) Serve() {

	s.closeFun = s.router.StartListen(s.addr, []string{s.topic}, s.group, s.offset)
	s.discover.Register(ServiceDiscoveryTopic)
	s.discover.CheckAllService()

	sigs := make(chan os.Signal, 1)
	s.done = make(chan bool, 1)

	signal.Notify(sigs, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)
	go func() {
		<-sigs

		//	log.Println(sig)
		s.done <- true
	}()

	log.Println("kofa server running ServerId:", s.GetServerId(), "Topic:", s.topic)
	<-s.done

	s.discover.Logout()
	s.discover.Close()
	s.closeFun()
}

func (s *Server) Close() {
	s.done <- true

}

func (s *Server) Call(msgId uint64, key string, data []byte, topic ...string) error {
	service, ok := s.discover.GetServices()[msgId]

	if !ok {
		return errors.New("not find msgId ")
	}

	call, _ := proto.Marshal(&apis.Call{MsgId: msgId, Producer: s.serverId})
	if len(topic) <= 0 {
		s.send.Async(service.Topic, []byte(key), data, sarama.RecordHeader{Key: call})
		return nil
	}
	for _, v := range topic {
		s.send.Async(v, []byte(key), data, sarama.RecordHeader{Key: call})
	}
	return nil
}

func (s *Server) Send() kofa.ISend {
	return s.send
}
func (s *Server) GetServerId() string {
	return s.serverId
}
