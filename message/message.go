package message

import (
	"kofa/apis"
)

type Message interface {
	Header() Header
	Property() Property
	GetKey() string
	GetData() []byte
	SetKey(key string)
	SetData(data []byte)
}

type IMessage struct {
	header Header
	pro    Property
	Topic  []string
	Key    string
	Data   []byte
}

func NewMessage() Message {

	api := &apis.MessageHeader{Property: make(map[string][]byte)}

	return &IMessage{
		pro: &IProperty{
			MessageHeader: api},
		header: &IHeader{MessageHeader: api},
	}
}

func (msg *IMessage) GetKey() string  { return msg.Key }
func (msg *IMessage) GetData() []byte { return msg.Data }

func (msg *IMessage) Header() Header     { return msg.header }
func (msg *IMessage) Property() Property { return msg.pro }

func (msg *IMessage) SetKey(key string)   { msg.Key = key }
func (msg *IMessage) SetData(data []byte) { msg.Data = data }
