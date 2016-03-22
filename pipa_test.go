package pipa

import (
	"encoding/json"
	"testing"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"gopkg.in/Shopify/sarama.v1"
)

type testHandler struct {
	name   string
	cycles int
	events int
	policy RetryPolicy
}

func newTestHandler(name string) *testHandler {
	return &testHandler{name: name}
}

func (t *testHandler) Policy() RetryPolicy { return t.policy }
func (t *testHandler) Name() string        { return t.name }
func (t *testHandler) Process(batch EventBatch) (int, error) {
	t.cycles++
	t.events += len(batch)
	return len(batch), nil
}

// --------------------------------------------------------------------

type testParser struct {
	policy RetryPolicy
	cycles int
}

func newTestParser() *testParser          { return &testParser{} }
func (t *testParser) Policy() RetryPolicy { return t.policy }
func (t *testParser) Parse(m *sarama.ConsumerMessage) (interface{}, error) {
	t.cycles++

	var v testStruct
	if err := json.Unmarshal(m.Value, &v); err != nil {
		return nil, err
	}
	return &v, nil
}

// --------------------------------------------------------------------

func TestSuite(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "pipa")
}
