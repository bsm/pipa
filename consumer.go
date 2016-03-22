package pipa

import (
	"gopkg.in/bsm/sarama-cluster.v2"
	"gopkg.in/Shopify/sarama.v1"
)

// Consumer interface
type Consumer interface {
	Messages() <-chan *sarama.ConsumerMessage
	MarkOffset(*sarama.ConsumerMessage, string)
	Close() error
}

// NewConsumer connects to a real consumer
func NewConsumer(addrs []string, groupID string, topics []string, config *cluster.Config, notifier Notifier) (Consumer, error) {
	consumer, err := cluster.NewConsumer(addrs, groupID, topics, config)
	if err != nil {
		return nil, err
	}

	// process consumer errors
	go func() {
		for err := range consumer.Errors() {
			notifier.ConsumerError(err)
		}
	}()

	// process consumer notifications
	go func() {
		for ntfy := range consumer.Notifications() {
			notifier.ClaimedTopics(ntfy.Current)
		}
	}()

	return consumer, nil
}
