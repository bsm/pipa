package pipa

import (
	"sync"
	"time"
)

// InputStream reads messages from the consumer
type InputStream struct {
	consumer Consumer
	notifier Notifier
}

// NewInputStream wraps a consumer into a message stream
func NewInputStream(consumer Consumer, notifier Notifier) *InputStream {
	return &InputStream{consumer: consumer, notifier: notifier}
}

// Parse parses messages of the message streams and converts them into an event stream
func (s *InputStream) Parse(bufferSize int, parser Parser) *EventStream {
	events := make(chan *Event, bufferSize)
	policy := parser.Policy()
	go func() {
		defer close(events)

		for msg := range s.consumer.Messages() {
			_ = policy.Perform(func() error {
				value, err := parser.Parse(msg)
				if err != nil {
					s.notifier.ParseError(err)
				} else {
					events <- &Event{Value: value, Message: msg}
				}
				return err
			})
		}
	}()
	return &EventStream{events: events, consumer: s.consumer, notifier: s.notifier}
}

// --------------------------------------------------------------------

// EventStream streams individual events
type EventStream struct {
	events   <-chan *Event
	consumer Consumer
	notifier Notifier
}

// Drain drains the stream, blocking
func (s *EventStream) Drain() (n int) {
	for _ = range s.events {
		n++
	}
	return
}

// Batch creates batches of events using window intervals
func (s *EventStream) Batch(window time.Duration) *BatchStream {
	batches := make(chan EventBatch, 1)
	go func() {
		defer close(batches)

		var acc EventBatch
		for {
			select {
			case evt, ok := <-s.events:
				if !ok {
					if len(acc) != 0 {
						batch := make(EventBatch, len(acc))
						copy(batch, acc)
						batches <- batch
						acc = acc[:0]
					}
					return
				}
				acc = append(acc, *evt)
			case <-time.After(window):
				if len(acc) != 0 {
					batch := make(EventBatch, len(acc))
					copy(batch, acc)
					batches <- batch
					acc = acc[:0]
				}
			}
		}
	}()
	return &BatchStream{batches: batches, consumer: s.consumer, notifier: s.notifier}
}

// --------------------------------------------------------------------

// BatchStream streams batches of events
type BatchStream struct {
	batches  <-chan EventBatch
	consumer Consumer
	notifier Notifier
}

// Drain drains the stream, blocking
func (s *BatchStream) Drain() (n, m int) {
	for batch := range s.batches {
		n++
		m += len(batch)
	}
	return
}

// Process distributes the steam across handlers, blocking
func (s *BatchStream) Process(handlers ...Handler) {
	stopper := new(sync.WaitGroup)
	monitor := new(sync.WaitGroup)
	children := make([]handlerProcess, len(handlers))

	for i := 0; i < len(handlers); i++ {
		stopper.Add(1)
		children[i] = handlerProcess{
			handler:  handlers[i],
			batches:  make(chan EventBatch),
			notifier: s.notifier,
		}
		go children[i].loop(stopper, monitor)
	}

	for batch := range s.batches {
		for _, child := range children {
			monitor.Add(1)
			child.batches <- batch
		}
		monitor.Wait()

		for _, evt := range batch {
			s.consumer.MarkOffset(evt.Message, "")
		}
	}

	for _, child := range children {
		close(child.batches)
	}
	stopper.Wait()
}

// --------------------------------------------------------------------

type handlerProcess struct {
	handler  Handler
	batches  chan EventBatch
	notifier Notifier
}

func (s *handlerProcess) loop(stopper, monitor *sync.WaitGroup) {
	defer stopper.Done()

	policy := s.handler.Policy()
	for batch := range s.batches {
		_ = policy.Perform(func() (err error) {
			start := time.Now()
			if _, err = s.handler.Process(batch); err != nil {
				s.notifier.HandlerError(s.handler.Name(), len(batch), err)
			} else {
				s.notifier.HandlerOK(s.handler.Name(), len(batch), time.Since(start))
			}
			return
		})
		monitor.Done()
	}
}
