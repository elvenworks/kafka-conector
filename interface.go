package kafka

import "github.com/Shopify/sarama"

type IKafka interface {
	Produce(topic string, message []byte) error
	ProduceWithFallback(topic string, message []byte, erro error) error
	ConsumeWithFallback(topic, groupName string, maxBufferSize, numberOfRoutines int) (msgChannel chan *sarama.ConsumerMessage, err error)
	ProduceAndConsumeOnce(topic string, message []byte) error
	GetLag(topic, consumerGroup string) (lagTotal int64, err error)
	BatchConsumeWithFallback(topics []string, groupName string, maxBufferSize, numberOfRoutines int) (msgChannel chan *sarama.ConsumerMessage, err error)
}
