package producer_async

import "github.com/Shopify/sarama"

type IProducerAsync interface {
	GetConfig() *sarama.Config
	Produce(topic string, message interface{})
	ProduceWithMessageKey(topic string, key interface{}, message interface{})
}
