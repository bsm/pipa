package pipa

import "time"

type RetryPolicy struct {
	Times int
	Sleep time.Duration
}

func (p RetryPolicy) Perform(cb func() error) (err error) {
	for i := 0; i <= p.Times; i++ {
		if err = cb(); err == nil {
			return
		}
		if p.Sleep > 0 {
			time.Sleep(p.Sleep)
		}
	}
	return
}
