package factory

import (
	"github.com/Shopify/sarama"
	"github.com/elvenworks/kafka-conector/v2/internal/delivery/worker/consumer"
	consumerV1 "github.com/elvenworks/kafka-conector/v2/internal/delivery/worker/consumer/v1"
	consumerV2 "github.com/elvenworks/kafka-conector/v2/internal/delivery/worker/consumer/v2"
)

const (
	VERSION_V2 = "v2"
)

func NewConsumerGroup(version string, brokers []string, group string, config *sarama.Config) (consumer.IConsumer, error) {
	switch version {
	case VERSION_V2:
		return consumerV2.NewConsumerGroup(brokers, group, config)
	}

	return consumerV1.NewConsumerGroup(brokers, group, config)
}
