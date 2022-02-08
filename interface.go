package kafka

import "github.com/Shopify/sarama"

type IKafka interface {
	GetConfig() *sarama.Config
	Produce(topic string, message interface{})
	Consume(topic, groupName string, maxBufferSize, numberOfRoutines int) (msgChannel chan *sarama.ConsumerMessage, err error)
	BatchConsume(topics []string, groupName string, maxBufferSize, numberOfRoutines int) (msgChannel chan *sarama.ConsumerMessage, err error)
	ProduceSync(topic string, message interface{}) error
	ProduceAndConsumeOnce(topic string, message interface{}) error
	GetLag(topic, consumerGroup string) (lagTotal int64, err error)
}
