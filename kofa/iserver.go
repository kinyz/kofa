package kofa

type IServer interface {
	AddRouter(msgId uint64, alias string, obj interface{}, param ...interface{})
	CustomHandle(kafka IKafkaRequest)
	Serve()
	Close()
	Send() ISend
	Call(msgId uint64, key string, data []byte, topic ...string) error
	GetServerId() string
}
