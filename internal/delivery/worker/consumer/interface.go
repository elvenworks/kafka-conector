package consumer

type IConsumer interface {
	Consume(topic string, maxBufferSize int, numberOfRoutines int) (chan []byte, error)
}
