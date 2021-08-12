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

func NewConsumerGroup(brokers []string, group string, config *sarama.Config) (*ConsumerGroup, error) {
	client, err := sarama.NewConsumerGroup(brokers, group, config)
	if err != nil {
		return nil, err
	}

	return &ConsumerGroup{
		Consumer: client,
	}, nil
}

// Consume it's a MultiBatchConsumer
func (c *ConsumerGroup) Consume(topic string, maxBufferSize int, numberOfRoutines int) (chan []byte, error) {
	var count int64
	var start = time.Now()
	var bufChan = make(chan BatchMessages, 1000)
	var msgsChan = make(chan []byte)
	for i := 0; i < numberOfRoutines; i++ {
		go func(channelNumber int) {
			logrus.Infof("Channel *%v* started and consuming topic *%s*", channelNumber, topic)
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
	c.setHandler([]string{topic}, handler)
	return msgsChan, nil
}

func (c *ConsumerGroup) setHandler(topics []string, handler ConsumerGroupHandler) {
	ctx := context.Background()
	go func() {
		for {
			err := c.Consumer.Consume(ctx, topics, handler)
			if err != nil {
				if err == sarama.ErrClosedConsumerGroup {
					break
				} else {
					logrus.Fatal(err)
				}
			}
			if ctx.Err() != nil {
				logrus.Info("Error consuming message", ctx.Err())
				return
			}
			handler.Reset()
		}
	}()
}
