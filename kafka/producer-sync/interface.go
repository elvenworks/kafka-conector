package producer_sync

import "github.com/Shopify/sarama"

type IProducerSync interface {
	GetConfig() *sarama.Config
	ProduceSync(topic string, message interface{}) error
	ProduceSyncWithMessageKey(topic string, key interface{}, message interface{}) error
	ProduceAndConsumeOnce(topic string, message interface{}) error
}
