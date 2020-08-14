package kofa

import (
	"kofa/message"
)

type Request interface {
	Message() message.Message
	Send(msg message.Message, topic ...string) error
}

type IRequest struct {
	Msg    message.Message
	server Server
}

func NewRequest(server Server, msg message.Message) Request {
	return &IRequest{server: server, Msg: msg}
}
func (r *IRequest) Message() message.Message {
	return r.Msg
}

func (r *IRequest) Send(msg message.Message, topic ...string) error {
	return r.server.Send(msg, topic...)
}
