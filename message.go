package kofa

import (
	"kofa/message"
)

type Message interface {
	Initialize(server Server)
	Send(msg message.Message, topic ...string) error
	Listen(group string, topic ...string) error
	Close()
}
