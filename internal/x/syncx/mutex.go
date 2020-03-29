package syncx

import (
	"context"
	"sync"
)

// RWMutex is a context-aware read/write mutex.
type RWMutex struct {
	m        sync.Mutex
	readers  int // negative = write lock acquired
	unlocked chan struct{}
	retry    chan struct{}
}

// Lock acquires an exclusive lock on the mutex.
//
// It blocks until the mutex is acquired, or ctx is canceled.
func (m *RWMutex) Lock(ctx context.Context) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}

	m.m.Lock()

	if m.unlocked == nil {
		m.unlocked = make(chan struct{}, 1)
		m.unlocked <- struct{}{}
	}

	unlocked := m.unlocked

	m.m.Unlock()

	// Since we need an exclusive lock, we don't care how many readers/writers
	// there are, we just want to know when it's our turn.

	select {
	case <-ctx.Done():
		return ctx.Err()

	case <-unlocked:
		// We've obtained exclusive access, mark the mutex as "write-locked" by
		// sending the reader count negative.
		m.m.Lock()
		m.readers--
		m.m.Unlock()

		return nil
	}
}

// Unlock releases the mutex.
//
// It panics if the mutex is not currently locked with Lock().
func (m *RWMutex) Unlock() {
	m.m.Lock()

	if m.readers >= 0 {
		m.m.Unlock()
		panic("mutex is not write-locked")
	}

	m.readers++
	m.unlocked <- struct{}{}

	m.m.Unlock()
}

// RLock acquires a shared lock on the mutex.
//
// It blocks until the mutex is acquired, or ctx is canceled.
func (m *RWMutex) RLock(ctx context.Context) error {
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		m.m.Lock()

		// If there are already other readers, just add ourselves to the reader
		// count immediately.
		if m.readers > 0 {
			m.readers++
			m.m.Unlock()
			return nil
		}

		// Otherwise, we need to wait until we have exclusive access in order to
		// "convert" the mutex to read-locked.
		if m.unlocked == nil {
			m.unlocked = make(chan struct{}, 1)
			m.unlocked <- struct{}{}
		}

		// We also need to wake any other readers that come along.
		if m.retry == nil {
			m.retry = make(chan struct{})
		}

		unlocked := m.unlocked
		retry := m.retry

		// Release the internal mutex before waiting for exclusive access.
		m.m.Unlock()

		// And now we wait ...
		select {
		case <-ctx.Done():
			return ctx.Err()

		case <-retry:
			// Some other blocking call to RLock() obtained exclusive access
			// first, and notified us that the mutex is ready for reads.
			//
			// We have to retry from the beginning, in case the read-lock has
			// already been released.
			continue

		case <-unlocked:
			// We've obtained exclusive access, mark the mutex as "read-locked" by
			// sending the reader count positive.
			//
			// We then tell any other blocking RLock() calls to retry.
			m.m.Lock()

			m.readers++

			// If m.retry is already nil, it means that a competing goroutine
			// has already closed it and called RUnlock() after we unlocked the
			// internal mutex, but before we got to the select.
			//
			// See https://github.com/dogmatiq/infix/issues/72.
			if m.retry != nil {
				close(m.retry)
				m.retry = nil
			}

			m.m.Unlock()

			return nil
		}
	}
}

// RUnlock releases the mutex.
//
// It panics if the mutex is not currently locked with RLock().
func (m *RWMutex) RUnlock() {
	m.m.Lock()

	if m.readers <= 0 {
		m.m.Unlock()
		panic("mutex is not read-locked")
	}

	m.readers--

	if m.readers == 0 {
		m.unlocked <- struct{}{}
	}

	m.m.Unlock()
}
