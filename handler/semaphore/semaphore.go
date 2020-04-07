package semaphore

import (
	"context"

	"golang.org/x/sync/semaphore"
)

// Semaphore limits the number of messages that can be handled concurrently.
type Semaphore struct {
	n   int
	sem *semaphore.Weighted
}

// New returns a semaphore that allows n messages to be handled concurrently.
func New(n int) Semaphore {
	return Semaphore{
		n,
		semaphore.NewWeighted(int64(n)),
	}
}

// Limit returns the number of messages that can be handled concurrently.
//
// It returns 0 if there is no limit.
func (s *Semaphore) Limit() int {
	if s.sem == nil {
		return 0
	}

	return s.n
}

// Acquire blocks until it is ok for the caller to handled a message, or until
// ctx is canceled.
func (s *Semaphore) Acquire(ctx context.Context) error {
	if s.sem == nil {
		return nil
	}

	return s.sem.Acquire(ctx, 1)
}

// Release signals that an attempt at handling a message has completed.
func (s *Semaphore) Release() {
	if s.sem != nil {
		s.sem.Release(1)
	}
}
