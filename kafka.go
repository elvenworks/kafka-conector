package kafka

import (
	"encoding/json"
	"errors"
	"log"

	"github.com/Shopify/sarama"
	"github.com/elvenworks/kafka-conector/internal/delivery/worker/consumer"
	"github.com/elvenworks/kafka-conector/internal/delivery/worker/producer"
	"github.com/elvenworks/kafka-conector/internal/driver/kafka"
	"github.com/sirupsen/logrus"
)

type KafkaConfig struct {
	Brokers   []string
	User      string
	Password  string
	TLS       bool
	SASL      bool
	Mechanism string
	Auth      bool
}

type Kafka struct {
	brokers        []string
	Config         *sarama.Config
	producer       producer.IProducer
	clientConsumer consumer.IClientConsumer
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
		brokers: config.Brokers,
		Config:  brokerConfig,
	}
}

func (k *Kafka) GetConfig() *sarama.Config {
	return k.Config
}

func (k *Kafka) getProducer() producer.IProducer {
	if k.producer == nil {
		producer, err := producer.NewProducer(k.brokers, k.Config)
		if err != nil {
			log.Fatal(err)
		}
		k.producer = producer
	}
	return k.producer
}

func (k *Kafka) Produce(topic string, message []byte) {
	k.getProducer().Produce(topic, message)
}

func (k *Kafka) ProduceGrave(originTopic, serviceName string, message []byte, erro error) error {
	var payload map[string]interface{}
	if err := json.Unmarshal(message, &payload); err != nil {
		logrus.Errorf("Failed to send message to kafka, err: %s, msg: %s\n", err, message)
		return err
	}

	payload["errorConsuming"] = erro.Error()
	payload["graveServiceName"] = serviceName
	payload["graveOriginTopic"] = originTopic
	bytes, _ := json.Marshal(payload)

	k.getProducer().Produce("grave", bytes)
	return nil
}

func (k *Kafka) Consume(topic, groupName string, maxBufferSize, numberOfRoutines int) (msgChannel chan *sarama.ConsumerMessage, err error) {
	consumer, err := consumer.NewConsumerGroup(k.brokers, groupName, k.Config)
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
	consumer, err := consumer.NewConsumerGroup(k.brokers, groupName, k.Config)
	if err != nil {
		return nil, err
	}

	msgChan, err := consumer.MultiBatchConsumer(topics, maxBufferSize, numberOfRoutines)
	if err != nil {
		return nil, err
	}

	return msgChan, err
}

func (k *Kafka) ProduceAndConsumeOnce(topic string, message []byte) error {
	syncProducer, err := producer.NewSyncProducer(k.brokers, k.Config)
	if err != nil {
		return err
	}

	partition, offset, err := syncProducer.Produce(topic, message)
	if err != nil {
		return err
	}

	clientConsumer, err := consumer.NewClientConsumer(k.brokers, k.Config)
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

func (k *Kafka) GetLag(topic, consumerGroup string) (lagTotal int64, err error) {
	if k.clientConsumer == nil {
		k.clientConsumer, err = consumer.NewClientConsumer(k.brokers, k.Config)
		if err != nil {
			return 0, err
		}
	}
	return k.clientConsumer.GetLag(topic, consumerGroup)
}
