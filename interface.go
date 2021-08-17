package kafka

type IKafka interface {
	Produce(topic string, message []byte) error
	ProduceWithFallback(topic string, message []byte, erro error) error
	ConsumeWithFallback(topic, groupName string, maxBufferSize, numberOfRoutines int) (msgChannel chan []byte, err error)
}
