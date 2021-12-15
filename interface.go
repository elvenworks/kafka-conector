package kafka

import "github.com/Shopify/sarama"

type IKafka interface {
	GetConfig() *sarama.Config
	Produce(topic string, message []byte)
	ProduceGrave(originTopic, serviceName string, message []byte, erro error) error
	Consume(topic, groupName string, maxBufferSize, numberOfRoutines int) (msgChannel chan *sarama.ConsumerMessage, err error)
	BatchConsume(topics []string, groupName string, maxBufferSize, numberOfRoutines int) (msgChannel chan *sarama.ConsumerMessage, err error)
	ProduceAndConsumeOnce(topic string, message []byte) error
	GetLag(topic, consumerGroup string) (lagTotal int64, err error)
}
