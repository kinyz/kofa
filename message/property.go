package message

import (
	"errors"
	"github.com/golang/protobuf/proto"
	"kofa/apis"
	"sync"
)

type Property interface {
	Get(key string) ([]byte, error)
	Set(key string, value []byte)
	Remove(key string) error
	Clear()
	SetMap(propertyMap map[string][]byte)
	GetMap() map[string][]byte
}

type IProperty struct {
	proLock sync.RWMutex
	*apis.MessageHeader
}

func (p *IProperty) Get(key string) ([]byte, error) {
	p.proLock.RLock()
	defer p.proLock.RUnlock()
	if value, ok := p.Property[key]; ok {
		return value, nil
	} else {
		return nil, errors.New("key does not exist")

	}
}
func (p *IProperty) Set(key string, value []byte) {
	p.proLock.Lock()
	defer p.proLock.Unlock()
	p.Property[key] = value

}
func (p *IProperty) Remove(key string) error {
	p.proLock.Lock()
	defer p.proLock.Unlock()
	if _, ok := p.Property[key]; ok {
		delete(p.Property, key)
		return nil
	}
	return errors.New("key does not exist")
}
func (p *IProperty) GetMap() map[string][]byte {

	return p.Property
}
func (p *IProperty) SetMap(propertyMap map[string][]byte) {
	p.proLock.Lock()
	defer p.proLock.Unlock()
	p.Property = propertyMap
}

func (p *IProperty) Encode() ([]byte, error) { return proto.Marshal(p) }

func (p *IProperty) Clear() {
	p.proLock.Lock()
	defer p.proLock.Unlock()

	for k, _ := range p.MessageHeader.Property {
		delete(p.MessageHeader.Property, k)
	}

}
