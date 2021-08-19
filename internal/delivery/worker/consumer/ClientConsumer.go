package consumer

import (
	"github.com/Shopify/sarama"
)

type ClientConsumer struct {
	client   sarama.Client
	consumer sarama.Consumer
}

func NewClientConsumer(brokers []string, config *sarama.Config) (*ClientConsumer, error) {
	client, err := sarama.NewClient(brokers, config)
	if err != nil {
		return nil, err
	}

	consumer, err := sarama.NewConsumerFromClient(client)
	if err != nil {
		return nil, err
	}

	return &ClientConsumer{
		client:   client,
		consumer: consumer,
	}, nil
}

func (c *ClientConsumer) Consume(topic string, partition int32, offset int64) (msg []byte, erro error) {
	partitionConsumer, err := c.consumer.ConsumePartition(topic, partition, offset)
	if err != nil {
		return nil, err
	}

	for content := range partitionConsumer.Messages() {
		if err := partitionConsumer.Close(); err != nil {
			return nil, err
		}
		if err := c.consumer.Close(); err != nil {
			return nil, err
		}
		if err := c.client.Close(); err != nil {
			return nil, err
		}
		msg = content.Value
	}

	return msg, nil
}
