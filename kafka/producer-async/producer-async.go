package producer_async

import (
	"log"

	"github.com/Shopify/sarama"
	"github.com/elvenworks/kafka-conector/internal/delivery/worker/producer"
	"github.com/elvenworks/kafka-conector/internal/driver/kafka"
	"github.com/elvenworks/kafka-conector/kafka/common"
	"github.com/sirupsen/logrus"
)

type Kafka struct {
	brokers  []string
	Config   *sarama.Config
	producer producer.IProducer
}

func InitProducerAsync(config common.KafkaConfig) IProducerAsync {
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

	k.newAsyncProducer()
	logrus.Infof("[produce-async] %s - %+v", topic, message)
	k.producer.Produce(topic, message)

}

func (k *Kafka) ProduceWithMessageKey(topic string, key interface{}, message interface{}) {

	k.newAsyncProducer()
	logrus.Infof("[produce-async-with-key] %s - %+v", topic, message)
	k.producer.ProduceWithMessageKey(topic, key, message)
}

func (k *Kafka) newAsyncProducer() {
	if k.producer == nil {
		logrus.Infof("[produce-async] new producer")
		producer, err := producer.NewProducer(k.brokers, k.Config)
		if err != nil {
			log.Fatal(err)
		}
		k.producer = producer
	}

}
