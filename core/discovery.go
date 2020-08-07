package core

import (
	"fmt"
	"github.com/golang/protobuf/proto"
	"kofa/kface"
	"kofa/pd"
)

const (
	ServiceDiscoveryName  = "Discovery"       // 调用名
	ServiceDiscoveryTopic = "Kofa_Discovery_" // 用于监听请求

)

type Discovery struct {
	IRouter         *Router
	ISend           kface.ISend
	serverMap       map[string]map[string]string
	topicMap        map[string][]string
	methodMap       map[string]*pd.Discovery
	topic, serverId string
	upService       bool
}

func NewIDiscovery(r *Router, s kface.ISend, serverId string) *Discovery {
	return &Discovery{
		serverMap: make(map[string]map[string]string),
		topicMap:  make(map[string][]string),
		methodMap: make(map[string]*pd.Discovery),
		ISend:     s,
		IRouter:   r,
		serverId:  serverId,
	}
}

func (d *Discovery) Register(topic, serverId, discoveryTopic string) {
	for k, v := range d.IRouter.GetMethodMap() {
		for _, method := range v {
			service := &pd.Discovery{
				ServerId: serverId,
				Topic:    topic,
				Method:   method,
			}
			data, err := proto.Marshal(service)
			if err != nil {
				return
			}
			//fmt.Println("向服务中心注册服务",k,v)
			if k != ServiceDiscoveryName {
				call := serverId + "." + "Discovery.Add"
				_, _, err = d.ISend.Sync(discoveryTopic, call, data)
				if err != nil {
					fmt.Println("[服务发现] 注册服务失败:", k, v, err)
				}
			}

		}

	}
	d.topic = topic
}

func (d *Discovery) Logout() {
	for k, v := range d.IRouter.GetMethodMap() {
		for _, method := range v {
			service := &pd.Discovery{
				ServerId: d.serverId,
				Topic:    d.topic,
				Method:   method,
			}
			data, err := proto.Marshal(service)
			if err != nil {
				return
			}
			call := d.serverId + "." + "Discovery.Logout"
			//fmt.Println("向服务中心注销服务",k,v)

			_, _, err = d.ISend.Sync(ServiceDiscoveryTopic, call, data)
			if err != nil {
				fmt.Println("[服务发现] 删除服务失败:", k, v, err)
			}
		}

	}
}

func (d *Discovery) CheckAllService(receiveTopic string) {
	call := receiveTopic + "." + "Discovery.GetService"
	_, _, err := d.ISend.Sync(ServiceDiscoveryTopic, call, []byte(""))
	if err != nil {
		fmt.Println("[服务发现] 获取服务失败:", err)
	}

}

func (d *Discovery) Start(addr []string, b bool, serverId string) {
	d.upService = b
	d.IRouter.AddRouter(ServiceDiscoveryName, &DiscoveryRequest{d: d})
	d.IRouter.StartListen(addr, []string{ServiceDiscoveryTopic, d.serverId}, ServiceDiscoveryTopic+serverId, -1)
}
func (d *Discovery) Add(service *pd.Discovery) {

	if len(d.serverMap[service.Method]) <= 0 {
		d.serverMap[service.Method] = make(map[string]string)
		d.serverMap[service.Method][service.ServerId] = service.ServerId
		if d.methodMap[service.Method] == nil {
			d.methodMap[service.Method] = service
			d.topicMap[service.Topic] = append(d.topicMap[service.Topic], service.Method)
			fmt.Println("[服务发现]添加服务:", service.Topic, service.Method)
		}
	} else {

		if d.serverMap[service.Method][service.ServerId] != "" {
			return
		}
		//fmt.Println("[服务发现]注册服务:", service.Topic, service.Method,service.ServerId)
		d.serverMap[service.Method][service.ServerId] = service.ServerId
	}

}
func (d *Discovery) Del(service *pd.Discovery) {

	if len(d.serverMap[service.Method]) > 0 {
		delete(d.serverMap[service.Method], service.ServerId)
		//fmt.Println("[服务发现]注销服务:", service.Topic, service.Method,service.ServerId)

		if len(d.serverMap[service.Method]) <= 0 {
			//	delete(d.msgMap,service.MsgId)
			delete(d.methodMap, service.Method)
			fmt.Println("[服务发现]删除服务:", service.Topic, service.Method)
		}
	}

}
func (d *Discovery) GetTopicByMethod(method string) string {
	topic, ok := d.methodMap[method]
	if ok {
		return topic.Topic

	}
	return ""
}

func (d *Discovery) GetMethodByTopic(topic string) []string {
	return d.topicMap[topic]
}

type DiscoveryRequest struct {
	d *Discovery
}

func (dReq *DiscoveryRequest) Add(request kface.IRequest) {
	if dReq.d.upService {
		service := &pd.Discovery{}
		err := proto.Unmarshal(request.GetData(), service)
		if err != nil {
			return
		}
		dReq.d.Add(service)
	}

}
func (dReq *DiscoveryRequest) Logout(request kface.IRequest) {
	if dReq.d.upService {
		service := &pd.Discovery{}
		err := proto.Unmarshal(request.GetData(), service)
		if err != nil {
			return
		}
		dReq.d.Del(service)
	}
}

func (dReq *DiscoveryRequest) GetService(request kface.IRequest) {
	dReq.d.Register(dReq.d.topic, dReq.d.serverId, request.GetProducer())

}
