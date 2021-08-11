package producer

type IProducer interface {
	Produce(topic string, message []byte)
	Close() error
}
