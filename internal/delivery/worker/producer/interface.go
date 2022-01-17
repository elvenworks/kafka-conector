package producer

type IProducer interface {
	Produce(topic string, message []byte)
	Close() error
}

type ISyncProducer interface {
	Produce(topic string, message []byte) (partition int32, offset int64, err error)
	Close() error
}
