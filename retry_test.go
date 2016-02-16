package pipa

import (
	"io"
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("RetryPolicy", func() {

	It("should retry", func() {
		var n int

		err := (RetryPolicy{}).Perform(func() error {
			n++
			return io.EOF
		})
		Expect(err).To(Equal(io.EOF))
		Expect(n).To(Equal(1))

		err = (RetryPolicy{Times: 10, Sleep: time.Millisecond}).Perform(func() error {
			n++
			return io.EOF
		})
		Expect(err).To(Equal(io.EOF))
		Expect(n).To(Equal(12))

		err = (RetryPolicy{Times: 10, Sleep: time.Millisecond}).Perform(func() error {
			if n++; n == 20 {
				return nil
			}
			return io.EOF
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(n).To(Equal(20))
	})

})
