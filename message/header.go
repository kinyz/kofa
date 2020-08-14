package message

import (
	"github.com/golang/protobuf/proto"
	"kofa/apis"
)

type Header interface {
	GetMsgId() uint64
	GetProducer() string
	SetMsgId(msgId uint64)
	SetProducer(producer string)
	GetTimesTamp() int64
	SetTimesTamp(UnixNano int64)
	Encode() ([]byte, error)
	Decode(data []byte) error
}
type IHeader struct {
	*apis.MessageHeader
}

func (h *IHeader) GetMsgId() uint64    { return h.MsgId }
func (h *IHeader) GetProducer() string { return h.Producer }
func (h *IHeader) SetMsgId(msgId uint64) {
	h.MsgId = msgId
}
func (h *IHeader) SetProducer(producer string) {
	h.Producer = producer
}
func (h *IHeader) GetTimesTamp() int64         { return h.Timestamp }
func (h *IHeader) SetTimesTamp(UnixNano int64) { h.Timestamp = UnixNano }

func (h *IHeader) Encode() ([]byte, error) { return proto.Marshal(h) }

func (h *IHeader) Decode(data []byte) error { return proto.UnmarshalMerge(data, h.MessageHeader) }
