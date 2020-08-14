package kofa

import (
	"encoding/binary"
	"kofa/message"
	"log"
)

type Discovery interface {
	Initialize(server Server)
	Register()
	Logout()
	Check()
	Close()
}

type IDiscovery struct {
	Server
}

const DiscoveryTopic = "Kofa-Discovery"

func NewDiscovery() Discovery {
	return &IDiscovery{}
}

func (d *IDiscovery) Initialize(server Server) {
	d.Server = server
	d.Server.AddRouter(100, "Discovery", 0, &DiscoveryListen{serverId: make(map[uint64]map[string]bool)}, d.Server)
	err := d.Server.Message().Listen(DiscoveryTopic+d.Server.Info().GetId(), DiscoveryTopic, d.Server.Info().GetId())
	if err != nil {
		panic(err)
	}
}

func (d *IDiscovery) Register() {
	serMap := d.ServiceManager().GetAll()
	msg := message.NewMessage()
	msg.Header().SetMsgId(101)
	b64 := make([]byte, 20)
	b32 := make([]byte, 10)
	log.Println("discovery register begin...")

	for _, v := range serMap {
		if v.GetMsgId() <= 103 && v.GetMsgId() > 100 {
			continue
		}
		binary.BigEndian.PutUint64(b64, v.GetMsgId())
		binary.BigEndian.PutUint32(b32, v.GetLevel())
		msg.Property().Set("MsgId", b64)
		msg.Property().Set("Topic", []byte(v.GetTopic()))
		msg.Property().Set("Alias", []byte(v.GetAlias()))
		msg.Property().Set("Method", []byte(v.GetMethod()))
		msg.Property().Set("Level", b32)
		msg.Header().SetMsgId(101)
		err := d.Send(msg, DiscoveryTopic)

		if err != nil {
			log.Println(err)
		}
	}
}

func (d *IDiscovery) Logout() {
	serMap := d.ServiceManager().GetAll()
	msg := message.NewMessage()
	msg.Header().SetMsgId(103)
	b64 := make([]byte, 20)
	for _, v := range serMap {
		binary.BigEndian.PutUint64(b64, v.GetMsgId())
		msg.Property().Set("MsgId", b64)
		err := d.Send(msg, DiscoveryTopic)
		if err != nil {
			log.Println("discovery logout err:", err)
		}
	}
}

func (d *IDiscovery) Check() {
	msg := message.NewMessage()
	msg.Header().SetMsgId(102)
	err := d.Send(msg, DiscoveryTopic)
	if err != nil {
		log.Println("discovery check err:", err)
	}
}

func (d *IDiscovery) Close() {
	d.Logout()
}

type DiscoveryListen struct {
	serverId map[uint64]map[string]bool
}

func (dl *DiscoveryListen) Add(request Request, server Server) {

	msgId, err := request.Message().Property().Get("MsgId")

	//log.Println("proMap:",request.Message().Property().Get())
	if err != nil {
		log.Println("discovery add service err1:", err)
	}
	topic, err2 := request.Message().Property().Get("Topic")
	if err2 != nil {
		log.Println("discovery add service err2:", err)
	}
	alias, err3 := request.Message().Property().Get("Alias")
	if err3 != nil {
		log.Println("discovery add service err3:", err)
	}
	method, err4 := request.Message().Property().Get("Method")
	if err4 != nil {
		log.Println("discovery add service err4:", err)
	}
	level, err5 := request.Message().Property().Get("Level")

	if err5 != nil {
		log.Println("discovery add service err5:", err)
	}

	if _, ok := dl.serverId[binary.BigEndian.Uint64(msgId)]; !ok {
		dl.serverId[binary.BigEndian.Uint64(msgId)] = make(map[string]bool)
	}

	dl.serverId[binary.BigEndian.Uint64(msgId)][request.Message().Header().GetProducer()] = true
	_ = server.ServiceManager().Add(binary.BigEndian.Uint64(msgId), string(topic), string(alias), string(method), binary.BigEndian.Uint32(level))

}

func (dl *DiscoveryListen) Logout(request Request, server Server) {
	if data, err := request.Message().Property().Get("MsgId"); err != nil {
		log.Println("discovery logout service err:", err)
	} else {
		msgId := binary.BigEndian.Uint64(data)
		if _, ok := dl.serverId[msgId][request.Message().Header().GetProducer()]; ok {
			delete(dl.serverId[msgId], request.Message().Header().GetProducer())
		}
		if len(dl.serverId[msgId]) <= 0 {
			err := server.ServiceManager().Remove(msgId)
			if err != nil {
				log.Println(err)
			}
		}
	}

}

func (dl *DiscoveryListen) GetAllService(request Request, server Server) {

	serMap := server.ServiceManager().GetAll()
	request.Message().Header().SetMsgId(101)
	b64 := make([]byte, 20)
	b32 := make([]byte, 10)
	for _, v := range serMap {
		binary.BigEndian.PutUint64(b64, v.GetMsgId())
		binary.BigEndian.PutUint32(b32, v.GetLevel())
		request.Message().Property().Set("MsgId", b64)
		request.Message().Property().Set("Topic", []byte(v.GetTopic()))
		request.Message().Property().Set("Alias", []byte(v.GetAlias()))
		request.Message().Property().Set("Method", []byte(v.GetMethod()))
		request.Message().Property().Set("Level", b32)
		request.Message().Header().SetMsgId(101)
		err := request.Send(request.Message(), request.Message().Header().GetProducer())
		if err != nil {
			log.Println(err)
		}
	}

}
