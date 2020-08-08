package ikofa

type IDiscovery interface {
	Start(addr []string, b bool)
	Register(discoveryTopic string)
	CheckAllService()
	Logout()
	Close()
	GetTopicByMethod(alias string, method string) string
	GetMethodByTopic(topic string) []string
}
