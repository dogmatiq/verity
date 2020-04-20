package syncx

import (
	"context"
	"sync"
	"sync/atomic"
)

// UnlockFunc is a function used to unlock a previously locked mutex.
type UnlockFunc func()

// MutexNamespace is a "namespace" of named, context-aware mutexes.
type MutexNamespace struct {
	m       sync.Mutex
	mutexes map[string]*nmutex
}

// nmutex is a named mutex.
type nmutex struct {
	guard   chan struct{} // buffered guard, write = lock, read = unlock
	lockers int64         // number of pending or successful Lock() calls
}

// Lock acquires an exclusive lock on the mutex with the given name.
//
// It returns an unlock function which must be called to unlock the mutex.
//
// If the mutex is already locked, Lock() blocks until it is unlocked, or ctx is
// canceled.
func (ns *MutexNamespace) Lock(ctx context.Context, n string) (UnlockFunc, error) {
	m := ns.get(n)

	select {
	case <-ctx.Done():
		ns.release(n, m)
		return nil, ctx.Err()

	case m.guard <- struct{}{}: // lock the mutex
		var once sync.Once
		return func() {
			once.Do(func() {
				<-m.guard // unlock the mutex
				ns.release(n, m)
			})
		}, nil
	}
}

// get returns the mutex with the given name, creating it if necessary.
func (ns *MutexNamespace) get(n string) *nmutex {
	ns.m.Lock()
	defer ns.m.Unlock()

	if ns.mutexes == nil {
		ns.mutexes = map[string]*nmutex{}
	} else if m, ok := ns.mutexes[n]; ok {
		atomic.AddInt64(&m.lockers, 1)
		return m
	}

	m := &nmutex{
		guard:   make(chan struct{}, 1),
		lockers: 1,
	}

	ns.mutexes[n] = m

	return m
}

// release decrements the locker count on m and removes it from ns.mutexes if
// there are no other lockers.
func (ns *MutexNamespace) release(n string, m *nmutex) {
	// Remove outselves from the locker count. If the result is non-zero then
	// there are other pending Lock() calls that will take ownership, so there's
	// nothing left to do.
	if atomic.AddInt64(&m.lockers, -1) > 0 {
		return
	}

	// Otherwise, we have to remove the mutex from the map.
	ns.m.Lock()

	// But before we do, we make sure no new locker came along while we were
	// waiting to acquire the ns.m. Note that we can check this here because
	// m.lockers is only INCREMENTED while ns.m is held.
	if atomic.LoadInt64(&m.lockers) == 0 {
		delete(ns.mutexes, n)
	}

	ns.m.Unlock()
}
