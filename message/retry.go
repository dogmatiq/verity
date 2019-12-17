package message

import (
	"math"
	"math/rand"
	"time"
)

// Attempt contains information about a specific attempt to handle a message.
type Attempt struct {
	// RetryCount is the number of times this message has been retried.
	// A value of zero indicates the first attempt.
	RetryCount uint32
}

// RetryPolicy is an interface for determining when failed messages should be
// retried.
type RetryPolicy interface {
	NextRetry(now time.Time, a Attempt, cause []error) time.Time
}

// DefaultRetryPolicy is the retry policy used when no custom policy is specified.
var DefaultRetryPolicy RetryPolicy = ExponentialBackoff{
	100 * time.Millisecond,
	1 * time.Hour,
	0.25,
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
	a Attempt,
	_ []error,
) time.Time {
	return now.Add(
		p.delay(a.RetryCount),
	)
}

// delay returns the time to delay a message that has failed on the n'th retry.
func (p ExponentialBackoff) delay(n uint32) time.Duration {
	s := math.Pow(2, float64(n)) * p.Min.Seconds()

	if s > p.Max.Seconds() {
		s = p.Max.Seconds()
	}

	s *= 1 + (rand.Float64() * p.Jitter)

	return time.Duration(
		s * float64(time.Second),
	)
}
