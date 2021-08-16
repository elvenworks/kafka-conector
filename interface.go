package kafka

type IKafka interface {
	Produce(topic string, message []byte, erro error) error
	Consume(topic, groupName string) (msgChannel []chan []byte, err error)
	ConsumeBulk(topic, groupName string, maxBufferSize, numberOfRoutines int) (msgChannel []chan []byte, err error)
}
