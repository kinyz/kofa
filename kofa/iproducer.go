package kofa

type ISend interface {
	Async(topic string, key, data []byte)
	Sync(topic string, key, data []byte) (int32, int64, error)
}
