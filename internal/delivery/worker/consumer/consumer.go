package consumer

import (
	"context"
	"fmt"
	"time"

	"github.com/Shopify/sarama"
	"github.com/sirupsen/logrus"
)

type ConsumerGroup struct {
	Consumer sarama.ConsumerGroup
}

func NewConsumerGroup() *ConsumerGroup {
	return &ConsumerGroup{}
}

// Consume it's a MultiBatchConsumer
func (c *ConsumerGroup) Consume(brokers []string, topic, group string, config *sarama.Config, maxBufferSize int, numberOfRoutines int) (chan []byte, error) {
	var count int64
	var start = time.Now()
	var bufChan = make(chan BatchMessages, 1000)
	var msgsChan = make(chan []byte)
	for i := 0; i < numberOfRoutines; i++ {
		go func(channelNumber int) {
			logrus.Infof("Channel %v started and consuming topic %s", channelNumber, topic)
			for messages := range bufChan {
				for j := range messages {
					msgsChan <- messages[j].Message.Value
					messages[j].Session.MarkMessage(messages[j].Message, "")
				}
				count += int64(len(messages))
				if count >= 1000 {
					logrus.Info(fmt.Sprintf("multi batch consumer consumed %d messages at speed %.2f/s\n", count, float64(count)/time.Since(start).Seconds()))
					start = time.Now()
					count = 0
				}
			}
		}(i)
	}
	handler := NewMultiBatchConsumerGroupHandler(&MultiBatchConsumerConfig{
		MaxBufSize: maxBufferSize,
		BufChan:    bufChan,
	})
	err := startConsume(brokers, topic, group, config, handler)
	if err != nil {
		return nil, err
	}
	return msgsChan, nil
}

func startConsume(brokers []string, topic, group string, config *sarama.Config, handler ConsumerGroupHandler) error {
	ctx := context.Background()
	client, err := sarama.NewConsumerGroup(brokers, group, config)
	if err != nil {
		logrus.Fatal(err)
		return err
	}

	go func() {
		for {
			err := client.Consume(ctx, []string{topic}, handler)
			if err != nil {
				if err == sarama.ErrClosedConsumerGroup {
					break
				} else {
					logrus.Fatal(err)
				}
			}
			if ctx.Err() != nil {
				return
			}
			handler.Reset()
		}
	}()

	handler.WaitReady() // Await till the consumer has been set up

	return nil
}
