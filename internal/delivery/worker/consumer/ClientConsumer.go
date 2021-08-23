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

func (c *ClientConsumer) GetLag(topic, consumerGroup string) (lagTotal int64, err error) {

	partitions, err := c.consumer.Partitions(topic)
	if err != nil {
		return 0, err
	}

	var manager sarama.OffsetManager
	var partitionManager sarama.PartitionOffsetManager

	for partition := range partitions {

		manager, err = sarama.NewOffsetManagerFromClient(consumerGroup, c.client)
		if err != nil {
			return 0, err
		}

		partitionManager, err = manager.ManagePartition(topic, int32(partition))
		if err != nil {
			return 0, err
		}

		consumerGroupOffset, _ := partitionManager.NextOffset()

		topicOffset, err := c.client.GetOffset(topic, int32(partition), sarama.ReceiveTime)
		if err != nil {
			return 0, err
		}

		lagTotal += (topicOffset - consumerGroupOffset)
	}

	err = partitionManager.Close()
	if err != nil {
		return 0, err
	}

	err = manager.Close()
	if err != nil {
		return 0, err
	}

	err = c.consumer.Close()
	if err != nil {
		return 0, err
	}

	err = c.client.Close()
	if err != nil {
		return 0, err
	}

	return lagTotal, nil

}
