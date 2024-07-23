package consumer

import "github.com/Shopify/sarama"

type IConsumer interface {
	GetConfig() *sarama.Config
	Consume(topic, groupName string, maxBufferSize, numberOfRoutines int) (msgChannel chan *sarama.ConsumerMessage, err error)
	ConsumeOffsetOldest(topic, groupName string, maxBufferSize, numberOfRoutines int) (msgChannel chan *sarama.ConsumerMessage, err error)
	BatchConsume(topics []string, groupName string, maxBufferSize, numberOfRoutines int) (msgChannel chan *sarama.ConsumerMessage, err error)
	GetLag(topic, consumerGroup string) (lagTotal int64, err error)
}
