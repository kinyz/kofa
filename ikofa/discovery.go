package ikofa

import (
	"fmt"
	"github.com/golang/protobuf/proto"
	"kofa/kofa"
	"kofa/pd"
)

const (
	ServiceDiscoveryName  = "Discovery"      // 调用名
	ServiceDiscoveryTopic = "Kofa-Discovery" // 用于监听请求

)

type Discovery struct {
	IS        *Server
	serverMap map[string]map[string]string
	topicMap  map[string][]string
	methodMap map[string]*apis.Service
	upService bool
	close     func()
}

func NewIDiscovery(s *Server) *Discovery {
	return &Discovery{
		serverMap: make(map[string]map[string]string),
		topicMap:  make(map[string][]string),
		methodMap: make(map[string]*apis.Service),
		IS:        s,
	}

}

func (d *Discovery) Register(Topic string) {
	for k, v := range d.IS.router.GetServiceMap() {
		for _, method := range v {
			data, err := proto.Marshal(method)
			if err != nil {
				fmt.Println("[服务发现] 注册服务失败1:", method, err)
			}
			//fmt.Println("向服务中心注册服务",k,v)
			if k != ServiceDiscoveryName {
				err = d.IS.Call(ServiceDiscoveryName, "Add", data, Topic)
				if err != nil {
					fmt.Println("[服务发现] 注册服务失败:", k, v, err)
				}
			}

		}

	}
}

func (d *Discovery) Logout() {
	for k, v := range d.IS.router.GetServiceMap() {
		for _, method := range v {
			data, err := proto.Marshal(method)
			if err != nil {
				fmt.Println("[服务发现] 注销服务失败:", method, err)
			}
			//fmt.Println("向服务中心注册服务",k,v)
			if k != ServiceDiscoveryName {
				err = d.IS.Call(ServiceDiscoveryName, "Logout", data, ServiceDiscoveryTopic)
				if err != nil {
					fmt.Println("[服务发现] 删除服务失败:", k, v, err)
				}
			}

		}
	}
}

func (d *Discovery) CheckAllService() {

	err := d.IS.Call(ServiceDiscoveryName, "GetService", []byte(""), ServiceDiscoveryTopic)
	if err != nil {
		fmt.Println("[服务发现] 获取服务失败:", err)
	}

}

func (d *Discovery) Start(addr []string, b bool) {
	d.upService = b
	d.IS.AddRouter(ServiceDiscoveryName, &DiscoveryRequest{d: d})
	d.close = d.IS.router.StartListen(addr, []string{ServiceDiscoveryTopic, d.IS.serverId}, ServiceDiscoveryTopic+d.IS.serverId, -1)
}
func (d *Discovery) Close() {
	d.close()
}

func (d *Discovery) Add(service *apis.Service) {
	path := service.Alias + "." + service.Method

	if len(d.serverMap[path]) <= 0 {
		d.serverMap[path] = make(map[string]string)
		d.serverMap[path][service.ServerId] = service.ServerId
		if d.methodMap[path] == nil {
			d.methodMap[path] = service
			d.topicMap[service.Topic] = append(d.topicMap[service.Topic], path)
			fmt.Println("[服务发现]添加服务:", service.Topic, path)
		}
	} else {

		if d.serverMap[path][service.ServerId] != "" {
			return
		}
		d.serverMap[path][service.ServerId] = service.ServerId
	}

}
func (d *Discovery) Del(service *apis.Service) {
	path := service.Alias + "." + service.Method

	if len(d.serverMap[path]) > 0 {
		delete(d.serverMap[path], service.ServerId)

		if len(d.serverMap[path]) <= 0 {
			//	delete(d.msgMap,service.MsgId)
			delete(d.methodMap, path)
			fmt.Println("[服务发现]删除服务:", service.Topic, path)
		}
	}

}
func (d *Discovery) GetTopicByMethod(alias, method string) string {
	topic, ok := d.methodMap[alias+"."+method]
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

func (dReq *DiscoveryRequest) Add(request kofa.Request) {
	if dReq.d.upService {
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

	dReq.d.Register(request.GetProducer())

}
