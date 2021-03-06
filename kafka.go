package kafka

import (
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
	syncProducer   producer.ISyncProducer
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

func (k *Kafka) Produce(topic string, message interface{}) {
	if k.producer == nil {
		producer, err := producer.NewProducer(k.brokers, k.Config)
		if err != nil {
			log.Fatal(err)
		}
		k.producer = producer
	}
	k.producer.Produce(topic, message)
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

func (k *Kafka) ProduceSync(topic string, message interface{}) error {
	var err error
	k.syncProducer, err = producer.NewSyncProducer(k.brokers, k.Config)
	if err != nil {
		return err
	}
	defer k.syncProducer.Close()

	_, _, err = k.syncProducer.Produce(topic, message)
	if err != nil {
		return err
	}
	return err
}

func (k *Kafka) ProduceAndConsumeOnce(topic string, message interface{}) error {
	var err error

	k.syncProducer, err = producer.NewSyncProducer(k.brokers, k.Config)
	if err != nil {
		return err
	}
	defer k.syncProducer.Close()

	partition, offset, err := k.syncProducer.Produce(topic, message)
	if err != nil {
		return err
	}

	k.clientConsumer, err = consumer.NewClientConsumer(k.brokers, k.Config)
	if err != nil {
		return err
	}
	defer k.clientConsumer.Close()

	msg, err := k.clientConsumer.Consume(topic, partition, offset)
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

	k.clientConsumer, err = consumer.NewClientConsumer(k.brokers, k.Config)
	if err != nil {
		return 0, err
	}
	defer k.clientConsumer.Close()

	lagTotal, err = k.clientConsumer.GetLag(topic, consumerGroup)

	return lagTotal, err
}
