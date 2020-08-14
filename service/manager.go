package service

import (
	"errors"
	"log"
	"sync"
)

type Manager interface {
	Add(msgId uint64, Topic, alias, method string, level uint32) error
	Remove(msgId uint64) error
	Get(msgId uint64) (Service, error)
	GetAll() map[uint64]Service
	Len() int
	Upgrade(msgId uint64, name, alias, method string, level uint32) error
	Clear()
}

type SerManager struct {
	services map[uint64]Service
	serLock  sync.RWMutex
}

func NewManager() Manager {
	return &SerManager{
		services: make(map[uint64]Service),
	}
}
func (serMgr *SerManager) Add(msgId uint64, Topic, alias, method string, level uint32) error {
	//log.Println("serMgr:",msgId,Topic,alias,method,level)
	serMgr.serLock.Lock()
	defer serMgr.serLock.Unlock()
	if _, ok := serMgr.services[msgId]; ok {

		return errors.New("add service error msgId already exists")
	}
	serMgr.services[msgId] = NewService(msgId, Topic, alias, method, level)
	log.Println("add service ", msgId, alias, method)
	return nil
}

func (serMgr *SerManager) Remove(msgId uint64) error {
	serMgr.serLock.Lock()
	defer serMgr.serLock.Unlock()
	_, ok := serMgr.services[msgId]
	if !ok {
		return errors.New("remove service error msgId does not exist ")
	}
	delete(serMgr.services, msgId)
	log.Println("remove service ", msgId)
	return nil
}

func (serMgr *SerManager) Get(msgId uint64) (Service, error) {
	serMgr.serLock.RLock()
	defer serMgr.serLock.RUnlock()

	if ser, ok := serMgr.services[msgId]; ok {
		return ser, nil
	} else {
		return nil, errors.New("get service error msgId does not exist")
	}
}

func (serMgr *SerManager) GetAll() map[uint64]Service {
	return serMgr.services
}

func (serMgr *SerManager) Len() int {
	return len(serMgr.services)
}

func (serMgr *SerManager) Upgrade(msgId uint64, name, alias, method string, level uint32) error {
	serMgr.serLock.Lock()
	defer serMgr.serLock.Unlock()
	if _, ok := serMgr.services[msgId]; ok {
		serMgr.services[msgId] = NewService(msgId, name, alias, method, level)
		log.Println("upgrade service ", msgId)

		return nil
	} else {
		return errors.New("upgrade service error msgId does not exist")
	}
}

func (serMgr *SerManager) Clear() {
	serMgr.serLock.Lock()
	defer serMgr.serLock.Unlock()
	for k, _ := range serMgr.services {
		log.Println("remove service ", k)
		delete(serMgr.services, k)
	}
	log.Println("clear service done")
}
