package pipa

import "time"

// Notifier implements process callbacks, for logging and instrumentation
type Notifier interface {
	ClaimedTopics(map[string][]int32)
	ConsumerError(error)
	ParseError(error)
	HandlerError(string, int, error)
	HandlerOK(string, int, time.Duration)
}

// --------------------------------------------------------------------

func MultiNotify(other ...Notifier) Notifier {
	return multiNotifier(other)
}

type multiNotifier []Notifier

func (m multiNotifier) ClaimedTopics(topic map[string][]int32) {
	for _, i := range m {
		i.ClaimedTopics(topic)
	}
}
func (m multiNotifier) ConsumerError(err error) {
	for _, i := range m {
		i.ConsumerError(err)
	}
}
func (m multiNotifier) ParseError(err error) {
	for _, i := range m {
		i.ParseError(err)
	}
}
func (m multiNotifier) HandlerError(name string, n int, err error) {
	for _, i := range m {
		i.HandlerError(name, n, err)
	}
}
func (m multiNotifier) HandlerOK(name string, n int, d time.Duration) {
	for _, i := range m {
		i.HandlerOK(name, n, d)
	}
}

// --------------------------------------------------------------------

// StdLogger is an interface to log.Logger
type StdLogger interface {
	Printf(format string, args ...interface{})
}

type stdLogNotifier struct {
	StdLogger
}

// NewStdLogNotifier returns a notifier which uses the standard log
func NewStdLogNotifier(logger StdLogger) Notifier {
	return &stdLogNotifier{StdLogger: logger}
}

func (l *stdLogNotifier) ClaimedTopics(topics map[string][]int32) {
	l.Printf("claimed topics: %v", topics)
}
func (l *stdLogNotifier) ConsumerError(err error) {
	l.Printf("consumer error: %s", err.Error())
}
func (l *stdLogNotifier) ParseError(err error) {
	l.Printf("parse error: %s", err.Error())
}
func (l *stdLogNotifier) HandlerError(name string, n int, err error) {
	l.Printf("%s error on processing %d events: %s", name, n, err.Error())
}
func (l *stdLogNotifier) HandlerOK(name string, n int, d time.Duration) {
	l.Printf("%s processed %d events in %v", name, n, d)
}
