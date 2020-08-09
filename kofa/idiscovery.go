package kofa

type IDiscovery interface {
	Start(addr []string, b bool)
	Register(discoveryTopic string)
	CheckAllService()
	Logout()
	Close()
	GetMethodByTopic(topic string) []string
}
