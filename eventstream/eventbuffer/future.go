package eventbuffer

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/dogmatiq/infix/parcel"
)

type future struct {
	done     uint32     // atomic bool, fast path, protects parcel field
	m        sync.Mutex // slow path, protects parcel and resolved channel
	parcel   *parcel.Parcel
	resolved chan struct{}
}

// await blocks until the future is resolved, then returns the parcel it was
// resolved with.
func (f *future) await(ctx context.Context) (*parcel.Parcel, error) {
	if atomic.LoadUint32(&f.done) == 1 {
		// The "fast path" was successful, we know there's a parcel, no need to
		// read from the resolved channel.
		return f.parcel, nil
	}

	// Otherwise, we take the "slow path" of locking the mutex.
	f.m.Lock()

	if f.parcel != nil {
		// The future was resolved while we were waiting for the mutex, still
		// want to avoid the channel if possible, so return immediately.
		f.m.Unlock()
		return f.parcel, nil
	}

	if f.resolved == nil {
		// We're the first caller to await, so we need to initialize the
		// channel.
		f.resolved = make(chan struct{})
	}

	f.m.Unlock()

	// At this point we know that f.resolved will never be written again, and
	// that f.parcel will never be written after f.resolved is closed, so
	// there's no more mutexes to worry about.
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-f.resolved:
		return f.parcel, nil
	}
}

// resolve wakes any blocked calls to Await() and causes them to return p.
func (f *future) resolve(p *parcel.Parcel) {
	f.m.Lock()

	f.parcel = p
	atomic.StoreUint32(&f.done, 1)

	if f.resolved != nil {
		close(f.resolved)
	}

	f.m.Unlock()
}
