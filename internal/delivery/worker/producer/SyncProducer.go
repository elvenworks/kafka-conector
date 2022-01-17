package producer

import (
	"github.com/Shopify/sarama"
)

type SyncProducer struct {
	producer sarama.SyncProducer
}

func NewSyncProducer(brokers []string, config *sarama.Config) (*SyncProducer, error) {
	var err error
	syncProducer := &SyncProducer{}
	config.Producer.Return.Successes = true
	syncProducer.producer, err = sarama.NewSyncProducer(brokers, config)
	if err != nil {
		return nil, err
	}
	return syncProducer, nil
}

func (p *SyncProducer) Produce(topic string, message []byte) (partition int32, offset int64, err error) {
	partition, offset, err = p.producer.SendMessage(
		&sarama.ProducerMessage{
			Topic: topic,
			Value: sarama.ByteEncoder(message),
		},
	)
	if err != nil {
		return 0, 0, err
	}

	return partition, offset, err
}

func (p *SyncProducer) Close() error {
	err := p.producer.Close()
	if err != nil {
		return err
	}
	return nil
}
