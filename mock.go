package kafka

import (
	"github.com/Shopify/sarama"
	"github.com/stretchr/testify/mock"
)

type KafkaMock struct {
	mock.Mock
}

func (k KafkaMock) GetConfig() *sarama.Config {
	a := k.Called()
	return a.Get(0).(*sarama.Config)
}

func (k KafkaMock) Produce(topic string, message []byte) error {
	a := k.Called(topic, message)
	return a.Error(0)
}

func (k KafkaMock) ProduceWithFallback(topic string, message []byte, erro error) error {
	a := k.Called(topic, message, erro)
	return a.Error(0)
}

func (k KafkaMock) ConsumeWithFallback(topic, groupName string, maxBufferSize, numberOfRoutines int) (msgChannel chan *sarama.ConsumerMessage, err error) {
	a := k.Called(topic, groupName, maxBufferSize, numberOfRoutines)
	return a.Get(0).(chan *sarama.ConsumerMessage), a.Error(1)
}

func (k KafkaMock) BatchConsumeWithFallback(topics []string, groupName string, maxBufferSize, numberOfRoutines int) (msgChannel chan *sarama.ConsumerMessage, err error) {
	a := k.Called(topics, groupName, maxBufferSize, numberOfRoutines)
	return a.Get(0).(chan *sarama.ConsumerMessage), a.Error(1)
}

func (k KafkaMock) ProduceAndConsumeOnce(topic string, message []byte) error {
	a := k.Called(topic, message)
	return a.Error(0)
}

func (k KafkaMock) GetLag(topic, consumerGroup string) (lagTotal int64, err error) {
	a := k.Called(topic, consumerGroup)
	return a.Get(0).(int64), a.Error(1)
}
