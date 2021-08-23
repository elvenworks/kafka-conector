package consumer

type IConsumer interface {
	MultiBatchConsumer(topic []string, maxBufferSize int, numberOfRoutines int) (chan []byte, error)
}

type IClientConsumer interface {
	Consume(topic string, partition int32, offset int64) (msg []byte, erro error)
	GetLag(topic, consumerGroup string) (lagTotal int64, err error)
}
