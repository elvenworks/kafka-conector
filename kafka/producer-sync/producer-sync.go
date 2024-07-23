package producer_sync

import (
	"errors"

	"github.com/Shopify/sarama"
	"github.com/elvenworks/kafka-conector/v2/internal/delivery/worker/consumer"
	consumerV1 "github.com/elvenworks/kafka-conector/v2/internal/delivery/worker/consumer/v1"
	"github.com/elvenworks/kafka-conector/v2/internal/delivery/worker/producer"
	"github.com/elvenworks/kafka-conector/v2/internal/driver/kafka"
	"github.com/elvenworks/kafka-conector/v2/kafka/config"
	"github.com/sirupsen/logrus"
)

type Kafka struct {
	brokers        []string
	Config         *sarama.Config
	clientConsumer consumer.IClientConsumer
	syncProducer   producer.ISyncProducer
}

func Init(config config.KafkaConfig) IProducerSync {
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

func (k *Kafka) ProduceSync(topic string, message interface{}) error {

	if err := k.newSyncProducer(); err != nil {
		return err
	}
	_, _, err := k.syncProducer.Produce(topic, message)
	if err != nil {
		return err
	}

	return err
}

func (k *Kafka) ProduceSyncWithMessageKey(topic string, key interface{}, message interface{}) error {

	if err := k.newSyncProducer(); err != nil {
		return err
	}
	_, _, err := k.syncProducer.ProduceWithMessageKey(topic, key, message)
	if err != nil {
		return err
	}

	return err
}

func (k *Kafka) ProduceAndConsumeOnce(topic string, message interface{}) error {
	if err := k.newSyncProducer(); err != nil {
		return err
	}

	partition, offset, err := k.syncProducer.Produce(topic, message)
	if err != nil {
		return err
	}

	k.clientConsumer, err = consumerV1.NewClientConsumer(k.brokers, k.Config)
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

func (k *Kafka) newSyncProducer() error {

	if k.syncProducer == nil {
		logrus.Infof("[produce-sync] new producer")
		syncProducer, err := producer.NewSyncProducer(k.brokers, k.Config)
		if err != nil {
			return err
		}
		k.syncProducer = syncProducer
	}
	return nil
}
