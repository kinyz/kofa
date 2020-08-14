package service

import (
	"kofa/apis"
	"sync"
)

type Service interface {
	GetMsgId() uint64
	GetTopic() string
	GetAlias() string
	GetMethod() string
	GetLevel() uint32
}

type IService struct {
	ser     *apis.Service
	keyLock sync.RWMutex
}

func NewService(msgId uint64, topic, alias, method string, level uint32) Service {
	return &IService{
		ser: &apis.Service{MsgId: msgId, Topic: topic, Alias: alias, Method: method, Level: level},
	}
}

func (s *IService) GetMsgId() uint64  { return s.ser.MsgId }
func (s *IService) GetTopic() string  { return s.ser.Topic }
func (s *IService) GetAlias() string  { return s.ser.Alias }
func (s *IService) GetMethod() string { return s.ser.Method }
func (s *IService) GetLevel() uint32  { return s.ser.Level }
