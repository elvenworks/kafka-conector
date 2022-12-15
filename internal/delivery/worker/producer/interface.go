package producer

type IProducer interface {
	Produce(topic string, message interface{})
	ProduceWithMessageKey(topic string, key interface{}, message interface{})
	Close() error
}

type ISyncProducer interface {
	Produce(topic string, message interface{}) (partition int32, offset int64, err error)
	ProduceWithMessageKey(topic string, key interface{}, message interface{}) (partition int32, offset int64, err error)
	Close() error
}
