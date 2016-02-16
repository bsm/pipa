package pipa

import "github.com/Shopify/sarama"

// --------------------------------------------------------------------

var _ Consumer = &testConsumer{}

type testConsumer struct {
	messages chan *sarama.ConsumerMessage
	lastMark *sarama.ConsumerMessage
}

func newTestConsumer(messages ...sarama.ConsumerMessage) *testConsumer {
	mch := make(chan *sarama.ConsumerMessage, len(messages))
	for i := range messages {
		m := messages[i]
		mch <- &m
	}
	close(mch)

	return &testConsumer{messages: mch}
}

func (c *testConsumer) Messages() <-chan *sarama.ConsumerMessage       { return c.messages }
func (c *testConsumer) MarkOffset(m *sarama.ConsumerMessage, _ string) { c.lastMark = m }
func (c *testConsumer) Close() error                                   { return nil }
