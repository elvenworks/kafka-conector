package consumer

type IConsumer interface {
	MultiBatchConsumer(topic []string, maxBufferSize int, numberOfRoutines int) (chan []byte, error)
}
