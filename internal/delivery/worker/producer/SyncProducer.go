package producer

import (
	"encoding/json"

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

func (p *SyncProducer) Produce(topic string, message interface{}) (partition int32, offset int64, err error) {

	bytes, _ := json.Marshal(message)

	partition, offset, err = p.producer.SendMessage(
		&sarama.ProducerMessage{
			Topic: topic,
			Value: sarama.ByteEncoder(bytes),
		},
	)
	if err != nil {
		return 0, 0, err
	}

	return partition, offset, err
}

func (p *SyncProducer) ProduceWithMessageKey(topic string, key interface{}, message interface{}) (partition int32, offset int64, err error) {

	bytesKey, _ := json.Marshal(key)
	bytes, _ := json.Marshal(message)

	partition, offset, err = p.producer.SendMessage(
		&sarama.ProducerMessage{
			Topic: topic,
			Key:   sarama.ByteEncoder(bytesKey),
			Value: sarama.ByteEncoder(bytes),
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
