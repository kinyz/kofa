package ikofa

import (
	"github.com/golang/protobuf/proto"
	"kofa/kofa"
	"kofa/pd"
	"log"
)

const (
	ServiceDiscoveryName  = "Discovery"      // 调用名
	ServiceDiscoveryTopic = "Kofa-Discovery" // 用于监听请求
	ServiceDiscoveryMsgId = 1000
)

type Discovery struct {
	IS *Server

	serviceMap map[uint64]*apis.Service
	serverMap  map[uint64]map[string]string
	upService  bool
	close      func()
}

func NewIDiscovery(s *Server) *Discovery {
	return &Discovery{
		serverMap: make(map[uint64]map[string]string),
		//topicMap:  make(map[string][]string),
		serviceMap: make(map[uint64]*apis.Service),
		IS:         s,
	}

}

func (d *Discovery) Register(Topic string) {
	for _, v := range d.IS.router.GetServiceMap() {

		data, err := proto.Marshal(v)

		if err == nil {
			if v.Alias != ServiceDiscoveryName {
				err = d.IS.Call(ServiceDiscoveryMsgId+1, d.IS.serverId, data, Topic)
				if err != nil {
					log.Println("[服务发现] 注册服务失败:", v.MsgId, v.Alias, v.Method, err)
				}
			}

		}

	}
}
func (d *Discovery) CheckAllService() {

	err := d.IS.Call(ServiceDiscoveryMsgId+2, d.IS.serverId, nil, ServiceDiscoveryTopic)
	if err != nil {
		log.Println("[服务发现] 获取服务失败:", err)
	}

}

func (d *Discovery) Logout() {
	for k, v := range d.IS.router.GetServiceMap() {

		//data, err := proto.Marshal(v)

		//log.Println("向服务中心注册服务",k,v)
		data, err := proto.Marshal(v)

		if err == nil {
			if v.Alias != ServiceDiscoveryName {
				err := d.IS.Call(ServiceDiscoveryMsgId+3, d.IS.serverId, data, ServiceDiscoveryTopic)
				if err != nil {
					log.Println("[服务发现] 注册服务失败:", k, v, err)
				}
			}
		}

	}
}

func (d *Discovery) Start(addr []string, b bool) {
	d.upService = b
	d.IS.router.AddRouter(1000, d.IS.serverId, d.IS.topic, ServiceDiscoveryName, &DiscoveryRequest{d: d})
	d.close = d.IS.router.StartListen(addr, []string{ServiceDiscoveryTopic, d.IS.serverId}, ServiceDiscoveryTopic+d.IS.serverId, -1)
}
func (d *Discovery) Close() {
	d.close()
}

func (d *Discovery) Add(service *apis.Service) {
	//path := service.Alias + "." + service.Method

	if len(d.serverMap[service.MsgId]) <= 0 {
		d.serverMap[service.MsgId] = make(map[string]string)
		d.serverMap[service.MsgId][service.ServerId] = service.ServerId
		d.serviceMap[service.MsgId] = service
		log.Println("[服务发现]添加服务:", service.MsgId, service.Alias, service.Method)
		return
	}

	if _, ok := d.serverMap[service.MsgId][service.ServerId]; !ok {
		d.serverMap[service.MsgId][service.ServerId] = service.ServerId
	}

}
func (d *Discovery) Del(service *apis.Service) {

	if len(d.serverMap[service.MsgId]) > 0 {

		for _, v := range d.serverMap[service.MsgId] {
			if v == service.ServerId {
				//log.Println("位置",k)
				delete(d.serverMap[service.MsgId], service.ServerId)
			}
		}

	}
	if len(d.serverMap[service.MsgId]) <= 0 {
		delete(d.serviceMap, service.MsgId)
		log.Println("[服务发现]删除服务:", service.MsgId, service.Alias, service.Method)
	}

}

func (d *Discovery) GetServices() map[uint64]*apis.Service {
	return d.serviceMap
}

type DiscoveryRequest struct {
	d *Discovery
}

func (dReq *DiscoveryRequest) Add(request kofa.Request) {

	if dReq.d.upService {
		//log.Println("header", request.GetMessage().GetHeaders())

		service := &apis.Service{}
		err := proto.Unmarshal(request.GetData(), service)
		if err != nil {
			return
		}
		dReq.d.Add(service)
	}

}
func (dReq *DiscoveryRequest) Logout(request kofa.Request) {
	if dReq.d.upService {
		service := &apis.Service{}
		err := proto.Unmarshal(request.GetData(), service)
		if err != nil {
			return
		}
		dReq.d.Del(service)
	}
}

func (dReq *DiscoveryRequest) GetService(request kofa.Request) {

	dReq.d.Register(request.GetKey())

}
