package consumer

import "github.com/Shopify/sarama"

type IConsumer interface {
	Consume(brokers []string, topic, group string, config *sarama.Config, maxBufferSize int, numberOfRoutines int) (chan []byte, error)
}
