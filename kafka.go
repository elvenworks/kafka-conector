package kafka

import (
	"encoding/json"
	"fmt"

	"github.com/Shopify/sarama"
	"github.com/elvenworks/kafka-conector/internal/delivery/worker/consumer"
	"github.com/elvenworks/kafka-conector/internal/delivery/worker/producer"
	"github.com/elvenworks/kafka-conector/internal/driver/kafka"
	"github.com/sirupsen/logrus"
)

type KafkaConfig struct {
	Brokers       []string
	User          string
	Password      string
	TLS           bool
	SASL          bool
	Mechanism     string
	Auth          bool
	FallbackTries int
}

type Kafka struct {
	brokers       []string
	config        *sarama.Config
	producer      producer.IProducer
	consumer      consumer.IConsumer
	fallbackTries int
}

func InitKafka(config KafkaConfig) *Kafka {
	brokerConfig := kafka.Config(
		config.User,
		config.Password,
		config.Mechanism,
		config.Auth,
		config.SASL,
		config.TLS,
	)

	return &Kafka{
		brokers:       config.Brokers,
		config:        brokerConfig,
		fallbackTries: config.FallbackTries,
	}
}

func (k *Kafka) Produce(topic string, message []byte, erro error) error {
	if k.producer == nil {
		producer, err := producer.NewProducer(k.brokers, k.config)
		if err != nil {
			return err
		}
		k.producer = producer
	}

	if erro == nil {
		k.producer.Produce(topic, message)
		return nil
	}

	var payload map[string]interface{}
	if err := json.Unmarshal(message, &payload); err != nil {
		k.Produce(topic, message, err)
		logrus.Errorf("Failed to send message to kafka, err: %s, msg: %s\n", err, message)
		return err
	}

	nTries := 1
	if payload["nTries"] != nil {
		nTries = int(payload["nTries"].(float64)) + 1
	}

	payload["nTries"] = nTries
	payload["errorConsuming"] = erro.Error()
	bytes, err := json.Marshal(payload)
	if err != nil {
		k.Produce(topic, message, err)
		logrus.Errorf("Failed to send message to kafka, err: %s, msg: %s\n", err, message)
		return err
	}

	if nTries > k.fallbackTries {
		k.producer.Produce(fmt.Sprintf("%s-grave", topic), bytes)
	} else {
		k.producer.Produce(fmt.Sprintf("%s-fallback", topic), bytes)
	}
	return nil
}

func (k *Kafka) Consume(topic, groupName string) (msgChannel chan []byte, err error) {
	return k.ConsumeBulk(topic, groupName, 1, 1)
}

func (k *Kafka) ConsumeBulk(topic, groupName string, maxBufferSize, numberOfRoutines int) (msgChannel chan []byte, err error) {
	if k.consumer == nil {
		consumer, err := consumer.NewConsumerGroup(k.brokers, groupName, k.config)
		if err != nil {
			return nil, err
		}
		k.consumer = consumer
	}

	msgChan, err := k.consumer.Consume(topic, maxBufferSize, numberOfRoutines)
	if err != nil {
		return nil, err
	}

	return msgChan, err
}
