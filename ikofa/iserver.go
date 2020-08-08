package ikofa

type IServer interface {
	AddRouter(alias string, obj interface{}, param ...interface{})
	CustomHandle(kafka IKafkaRequest)
	Serve()
	Close()
	Send() ISend
	Call(alias, method string, data []byte, service ...string) error
	GetServerId() string
}
