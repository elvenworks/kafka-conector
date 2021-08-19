package producer

import (
	"github.com/Shopify/sarama"
	"github.com/sirupsen/logrus"
)

type Producer struct {
	producer sarama.AsyncProducer
}

func NewProducer(brokers []string, config *sarama.Config) (*Producer, error) {
	producer, err := sarama.NewAsyncProducer(brokers, config)
	if err != nil {
		return nil, err
	}
	return &Producer{
		producer: producer,
	}, nil
}

func (p *Producer) Produce(topic string, message []byte) {

	select {
	case p.producer.Input() <- &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.ByteEncoder(message),
	}:
	case err := <-p.producer.Errors():
		logrus.Errorf("Failed to send message to kafka, err: %s, msg: %s\n", err, message)
		p.Produce(topic, message) // infinity
	}
}

func (p *Producer) Close() error {
	if p != nil {
		return p.producer.Close()
	}
	return nil
}
