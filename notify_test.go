package pipa

import (
	"io"
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("multiNotifier", func() {
	var subject Notifier
	var a, b *testNotifier

	BeforeEach(func() {
		a, b = newTestNotifier(), newTestNotifier()
		subject = MultiNotify(a, b)
	})

	It("should instrument on each", func() {
		subject.ConsumerError(io.EOF)
		subject.ParseError(io.EOF)
		subject.HandlerError("none", 4, io.EOF)
		subject.HandlerOK("none", 5, time.Second)

		Expect(a).To(Equal(&testNotifier{
			ConsumerErrors: 1,
			ParseErrors:    1,
			HandlerErrors:  map[string]int{"none": 4},
			HandlerOKs:     map[string]int{"none": 5},
		}))
		Expect(b).To(Equal(&testNotifier{
			ConsumerErrors: 1,
			ParseErrors:    1,
			HandlerErrors:  map[string]int{"none": 4},
			HandlerOKs:     map[string]int{"none": 5},
		}))
	})

})

// --------------------------------------------------------------------

var _ Notifier = &testNotifier{}

// testNotifier is a minimal instrumentor which only increments count
type testNotifier struct {
	ConsumerErrors, ParseErrors int
	HandlerErrors               map[string]int
	HandlerOKs                  map[string]int
}

func newTestNotifier() *testNotifier {
	return &testNotifier{
		HandlerErrors: make(map[string]int),
		HandlerOKs:    make(map[string]int),
	}
}

func (t *testNotifier) ClaimedTopics(_ map[string][]int32)            {}
func (t *testNotifier) ConsumerError(_ error)                         { t.ConsumerErrors++ }
func (t *testNotifier) ParseError(_ error)                            { t.ParseErrors++ }
func (t *testNotifier) HandlerError(name string, n int, _ error)      { t.HandlerErrors[name] += n }
func (t *testNotifier) HandlerOK(name string, n int, _ time.Duration) { t.HandlerOKs[name] += n }
