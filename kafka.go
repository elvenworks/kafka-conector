package kafka

import (
	"encoding/json"
	"errors"
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

func (k *Kafka) Produce(topic string, message []byte) error {
	return k.ProduceWithFallback(topic, message, nil)
}

func (k *Kafka) ProduceWithFallback(topic string, message []byte, erro error) error {
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
		k.ProduceWithFallback(topic, message, err)
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
		k.ProduceWithFallback(topic, message, err)
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

func (k *Kafka) ConsumeWithFallback(topic, groupName string, maxBufferSize, numberOfRoutines int) (msgChannel chan []byte, err error) {
	consumer, err := consumer.NewConsumerGroup(k.brokers, groupName, k.config)
	if err != nil {
		return nil, err
	}

	topics := []string{topic, fmt.Sprintf("%s-fallback", topic)}

	msgChan, err := consumer.MultiBatchConsumer(topics, maxBufferSize, numberOfRoutines)
	if err != nil {
		return nil, err
	}

	return msgChan, err
}

func (k *Kafka) ProduceAndConsumeOnce(topic string, message []byte) error {
	syncProducer, err := producer.NewSyncProducer(k.brokers, k.config)
	if err != nil {
		return err
	}

	partition, offset, err := syncProducer.Produce(topic, message)
	if err != nil {
		return err
	}

	clientConsumer, err := consumer.NewClientConsumer(k.brokers, k.config)
	if err != nil {
		return err
	}

	msg, err := clientConsumer.Consume(topic, partition, offset)
	if err != nil {
		return err
	}

	if msg == nil {
		return errors.New("any message has been consumed")
	}

	logrus.Info("Message consumed right after being produced: ", sarama.StringEncoder(msg))

	return nil
}
