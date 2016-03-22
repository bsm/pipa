package pipa

import "gopkg.in/Shopify/sarama.v1"

// Handler can process a batch of events
type Handler interface {
	Name() string
	Policy() RetryPolicy
	Process(EventBatch) (int, error)
}

// Parser can parse raw events
type Parser interface {
	Parse(*sarama.ConsumerMessage) (interface{}, error)
	Policy() RetryPolicy
}

// --------------------------------------------------------------------

// Event represents a single event
type Event struct {
	Value   interface{}
	Message *sarama.ConsumerMessage
}

// EventBatch represents a batch of events
type EventBatch []Event
