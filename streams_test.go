package pipa

import (
	"time"

	"github.com/Shopify/sarama"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Streams", func() {
	var consumer *testConsumer
	var parser *testParser
	var notifier *testNotifier

	BeforeEach(func() {
		consumer = newTestConsumer(
			sarama.ConsumerMessage{Topic: "a", Partition: 0, Offset: 1, Value: []byte(`{"s":"a/0/1"}`)},
			sarama.ConsumerMessage{Topic: "a", Partition: 1, Offset: 1, Value: []byte(`{"s":"a/1/1"}`)},
			sarama.ConsumerMessage{Topic: "b", Partition: 0, Offset: 1, Value: []byte(`{"s":"b/0/1"}`)},
			sarama.ConsumerMessage{Topic: "b", Partition: 1, Offset: 1, Value: []byte(`{"s":"b/1/1"}`)},
			sarama.ConsumerMessage{Topic: "a", Partition: 1, Offset: 2, Value: []byte(`{"s":"a/1/2"}`)},
			sarama.ConsumerMessage{Topic: "a", Partition: 0, Offset: 2, Value: []byte(`{"s":"a/0/2"}`)},
			sarama.ConsumerMessage{Topic: "b", Partition: 1, Offset: 2, Value: []byte(`{"s":"b/1/2"}`)},
			sarama.ConsumerMessage{Topic: "b", Partition: 0, Offset: 2, Value: []byte(`{"s":"b/0/2"}`)},
		)
		parser = newTestParser()
		notifier = newTestNotifier()
	})

	AfterEach(func() {
		Expect(consumer.Close()).ToNot(HaveOccurred())
	})

	It("should parse", func() {
		n := NewInputStream(consumer, notifier).
			Parse(1, parser).
			Drain()
		Expect(n).To(Equal(8))
		Expect(parser.cycles).To(Equal(8))
		Expect(notifier.ParseErrors).To(Equal(0))
	})

	It("should retry on parse errors", func() {
		consumer = newTestConsumer(
			sarama.ConsumerMessage{Topic: "a", Partition: 0, Offset: 1, Value: []byte(`{"s":BAD}`)},
			sarama.ConsumerMessage{Topic: "a", Partition: 1, Offset: 1, Value: []byte(`{"s":"a/1/1"}`)},
		)
		parser.policy = RetryPolicy{Times: 5}

		n := NewInputStream(consumer, notifier).
			Parse(1, parser).
			Drain()
		Expect(n).To(Equal(1))
		Expect(parser.cycles).To(Equal(7))
		Expect(notifier.ParseErrors).To(Equal(6))
	})

	It("should batch", func() {
		n, m := NewInputStream(consumer, notifier).
			Parse(1, parser).
			Batch(20 * time.Millisecond).
			Drain()
		Expect(n).To(Equal(1))
		Expect(m).To(Equal(8))
	})

	It("should handle", func() {
		a, b := newTestHandler("A"), newTestHandler("B")

		NewInputStream(consumer, notifier).
			Parse(1, parser).
			Batch(20*time.Millisecond).
			Process(a, b)
		Expect(a.cycles).To(Equal(1))
		Expect(a.events).To(Equal(8))
		Expect(b.cycles).To(Equal(1))
		Expect(b.events).To(Equal(8))
		Expect(notifier.ParseErrors).To(Equal(0))
		Expect(notifier.HandlerErrors).To(BeEmpty())
		Expect(notifier.HandlerOKs).To(Equal(map[string]int{"A": 8, "B": 8}))
	})

})

// --------------------------------------------------------------------

type testStruct struct {
	S string
}
