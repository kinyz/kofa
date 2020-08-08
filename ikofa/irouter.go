package ikofa

import apis "kofa/pd"

type IRouter interface {
	AddRouter(serverId, topic, alias string, obj interface{}, param ...interface{})
	StartListen(addr, topic []string, group string, offset int64) func()
	GetServiceMap() map[string][]*apis.Service
	CustomHandle(kafka IKafkaRequest)
}
