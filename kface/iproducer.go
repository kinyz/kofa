package kface

type ISend interface {
	Async(topic string, key string, data []byte)
	Sync(topic string, key string, data []byte) (int32, int64, error)
}
