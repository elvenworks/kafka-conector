package consumer

import (
	"github.com/Shopify/sarama"
	"github.com/elvenworks/kafka-conector/internal/delivery/worker/consumer"
	consumerV1 "github.com/elvenworks/kafka-conector/internal/delivery/worker/consumer/v1"
	"github.com/elvenworks/kafka-conector/internal/driver/kafka"
	factory "github.com/elvenworks/kafka-conector/internal/factory"
	"github.com/elvenworks/kafka-conector/kafka/common"
)

type Kafka struct {
	brokers              []string
	Config               *sarama.Config
	clientConsumer       consumer.IClientConsumer
	ConsumerGroupVersion string
}

func InitConsumer(config common.KafkaConfig) IConsumer {
	brokerConfig := kafka.Config(
		config.User,
		config.Password,
		config.Mechanism,
		config.Auth,
		config.SASL,
		config.TLS,
	)

	return &Kafka{
		brokers:              config.Brokers,
		Config:               brokerConfig,
		ConsumerGroupVersion: config.ConsumerGroupVersion,
	}
}

func (k *Kafka) GetConfig() *sarama.Config {
	return k.Config
}

func (k *Kafka) Consume(topic, groupName string, maxBufferSize, numberOfRoutines int) (msgChannel chan *sarama.ConsumerMessage, err error) {
	consumer, err := factory.NewConsumerGroup(k.ConsumerGroupVersion, k.brokers, groupName, k.Config)
	if err != nil {
		return nil, err
	}

	topics := []string{topic}

	msgChan, err := consumer.MultiBatchConsumer(topics, maxBufferSize, numberOfRoutines)
	if err != nil {
		return nil, err
	}

	return msgChan, err
}

func (k *Kafka) ConsumeOffsetOldest(topic, groupName string, maxBufferSize, numberOfRoutines int) (msgChannel chan *sarama.ConsumerMessage, err error) {

	config := *k.Config
	config.Consumer.Offsets.Initial = sarama.OffsetOldest
	consumer, err := factory.NewConsumerGroup(k.ConsumerGroupVersion, k.brokers, groupName, &config)
	if err != nil {
		return nil, err
	}

	topics := []string{topic}
	msgChan, err := consumer.MultiBatchConsumer(topics, maxBufferSize, numberOfRoutines)
	if err != nil {
		return nil, err
	}

	return msgChan, err
}

func (k *Kafka) BatchConsume(topics []string, groupName string, maxBufferSize, numberOfRoutines int) (msgChannel chan *sarama.ConsumerMessage, err error) {
	consumer, err := factory.NewConsumerGroup(k.ConsumerGroupVersion, k.brokers, groupName, k.Config)
	if err != nil {
		return nil, err
	}

	msgChan, err := consumer.MultiBatchConsumer(topics, maxBufferSize, numberOfRoutines)
	if err != nil {
		return nil, err
	}

	return msgChan, err
}

func (k *Kafka) GetLag(topic, consumerGroup string) (lagTotal int64, err error) {

	k.clientConsumer, err = consumerV1.NewClientConsumer(k.brokers, k.Config)
	if err != nil {
		return 0, err
	}
	defer k.clientConsumer.Close()

	lagTotal, err = k.clientConsumer.GetLag(topic, consumerGroup)

	return lagTotal, err
}
