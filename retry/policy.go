package retry

import (
	"math"
	"math/rand"
	"time"
)

// Policy is an interface for determining when failed messages should next
// be retried.
type Policy interface {
	NextRetry(now time.Time, retries int, cause []error) time.Time
}

// ExponentialBackoff is a retry policy that uses exponential backoff.
type ExponentialBackoff struct {
	Min    time.Duration
	Max    time.Duration
	Jitter float64
}

// NextRetry returns the time at which the message should next be retried.
func (p ExponentialBackoff) NextRetry(
	now time.Time,
	retries int,
	_ []error,
) time.Time {
	return now.Add(
		p.delay(retries),
	)
}

// delay returns the time to delay a message that has failed on the n'th retry.
func (p ExponentialBackoff) delay(n int) time.Duration {
	s := math.Pow(2, float64(n)) * p.Min.Seconds()

	if s > p.Max.Seconds() {
		s = p.Max.Seconds()
	}

	s *= 1 + (rand.Float64() * p.Jitter)

	return time.Duration(
		s * float64(time.Second),
	)
}
