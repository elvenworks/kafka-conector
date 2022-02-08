package producer

type IProducer interface {
	Produce(topic string, message interface{})
	Close() error
}

type ISyncProducer interface {
	Produce(topic string, message interface{}) (partition int32, offset int64, err error)
	Close() error
}
