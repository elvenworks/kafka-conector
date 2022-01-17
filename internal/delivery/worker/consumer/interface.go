package consumer

import "github.com/Shopify/sarama"

type IConsumer interface {
	MultiBatchConsumer(topic []string, maxBufferSize int, numberOfRoutines int) (chan *sarama.ConsumerMessage, error)
}

type IClientConsumer interface {
	Consume(topic string, partition int32, offset int64) (msg []byte, erro error)
	GetLag(topic, consumerGroup string) (lagTotal int64, err error)
	Close() error
}
