package kface

type IServer interface {
	AddRouter(name string, obj interface{}, param ...interface{})
	ListenRouter(group string, offset int64, topic ...string)
	Start()
	Serve()
	Close()
	Call(producer, method string, data []byte) error
	GetServerTopic() string
	Send() ISend
	GetServerId() string
	GetTopicByMethod(method string) string
	GetMethodByTopic(topic string) []string
}
