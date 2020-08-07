package kface

type IRouter interface {
	AddRouter(name string, obj interface{}, param ...interface{})
	GetMsgRouter(msgId uint32) string
	GetMsgRouterAll() map[uint32]string
	StartListen(topic []string, group string, offset int64) func()
}
