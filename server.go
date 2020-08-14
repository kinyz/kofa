package kofa

import (
	"github.com/satori/go.uuid"
	"kofa/message"
	"kofa/reflect"
	"kofa/service"
	"log"
	"os"
	"os/signal"
	"syscall"
)

type Server interface {
	AddRouter(msgId uint64, alias string, level uint32, obj interface{}, param ...interface{})
	Message() Message
	Send(msg message.Message, topic ...string) error
	Close()
	Serve()
	ServiceManager() service.Manager
	Reflect() reflect.Reflect
	Info() ServerInfo
}

type ServerInfo interface {
	GetId() string
	GetName() string
	GetGroup() string
}
type Info struct {
	Name, Id, Group string
}

func (s *Info) GetId() string {
	return s.Id
}
func (s *Info) GetGroup() string {
	return s.Group
}
func (s *Info) GetName() string {
	return s.Name
}

type IServer struct {
	ref       reflect.Reflect
	message   Message
	serMgr    service.Manager
	info      *Info
	done      chan bool
	discovery Discovery
}

func New(Name string, group bool, msgMd Message, discovery ...Discovery) Server {
	info := &Info{Name: Name}
	info.Id = Name + "-" + uuid.NewV4().String()
	info.Group = info.Id
	if group {
		info.Group = Name
	}
	s := &IServer{

		info:    info,
		message: msgMd,
		serMgr:  service.NewManager(),
		ref:     reflect.NewReflect()}

	s.discovery = NewDiscovery()
	if len(discovery) > 0 {
		log.Println("我没有来吧")
		s.discovery = discovery[0]
	}
	s.message.Initialize(s)

	return s
}

func (s *IServer) AddRouter(msgId uint64, alias string, level uint32, obj interface{}, param ...interface{}) {
	/*
		if msgId < 2000 {
			log.Println(alias + " Failed to add. MsgId cannot be less than 2000")
			panic(alias + " Failed to add. MsgId cannot be less than 2000")
			return
		}

	*/
	names, err := s.ref.AddObj(alias, obj, param...)
	if err != nil {
		return
	}
	for i, v := range names {
		err = s.serMgr.Add(msgId+uint64(i)+1, s.info.GetName(), alias, v, level)
		if err != nil {
			panic(err)
		}
	}

}

func (s *IServer) Send(msg message.Message, topic ...string) error {
	return s.message.Send(msg, topic...)

}

func (s *IServer) Serve() {

	err := s.message.Listen(s.info.GetGroup(), s.info.GetName())
	if err != nil {
		panic(err)
	}
	s.discovery.Initialize(s)
	s.discovery.Register()
	s.discovery.Check()
	sigs := make(chan os.Signal, 1)
	s.done = make(chan bool, 1)

	signal.Notify(sigs, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)
	go func() {
		<-sigs

		s.done <- true
	}()

	log.Println("kofa server running...")
	log.Println("ServerId:", s.info.GetId())
	<-s.done
	s.discovery.Close()

	s.message.Close()

}

func (s *IServer) ServiceManager() service.Manager {
	return s.serMgr
}

func (s *IServer) Reflect() reflect.Reflect {
	return s.ref
}
func (s *IServer) Message() Message {
	return s.message
}
func (s *IServer) Info() ServerInfo {
	return s.info
}

func (s *IServer) Close() {
	s.done <- true
}
