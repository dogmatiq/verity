package memorystream

import (
	"context"
	"math"
	"sync/atomic"
	"unsafe"

	"github.com/dogmatiq/infix/eventstream"
	"github.com/dogmatiq/infix/internal/pooling"
	"github.com/dogmatiq/infix/internal/x/containerx/pqueue"
	"github.com/dogmatiq/infix/parcel"
)

// node is a member of a singly-linked list of contiguous "batches" of events.
type node struct {
	begin   uint64
	end     uint64
	parcels []*parcel.Parcel

	done unsafe.Pointer // atomic (*chan struct{})
	refs int32          // atomic
	next *node
}

// acquire "locks" the node for reading.
func (n *node) acquire() bool {
	// Increment the reference count. It should swing positive. If it doesn't it
	// means that this node has already been invalidated.
	return atomic.AddInt32(&n.refs, 1) > 0
}

// release "unlocks" a previously call to acquire().
//
// n may be invalidated and must not be used again.
func (n *node) release() {
	// Decrement the reference count.
	if atomic.AddInt32(&n.refs, -1) > 0 {
		// There are still other references.
		return
	}

	// If we reached zero, try to set the ref-count hard-negative. If we're able
	// to do this no future acquire() calls will pass so we can return the
	// parcels slice to the pool.
	//
	// If we can't do this it means that a call to acquire() occured after we
	// decremented the reference count but before we attempted this CAS.
	if atomic.CompareAndSwapInt32(&n.refs, 0, math.MinInt32) {
		pooling.ParcelSlices.Put(n.parcels)
	}
}

// advance returns the next node in the list.
//
// It blocks until the next node is linked or or ctx is canceled.
//
// On success, n may be invalidated and must not be used again.
func (n *node) advance(
	ctx context.Context,
	closed <-chan struct{},
) (*node, error) {
	done := atomic.LoadPointer(&n.done)

	if done == nil {
		// There's no existing "wake" channel, we have to try to make our own.
		ch := pooling.DoneChannels.Get()
		done = unsafe.Pointer(&ch)

		if !atomic.CompareAndSwapPointer(&n.done, nil, done) {
			// Another goroutine beat us to the punch.
			pooling.DoneChannels.PutUnchecked(ch)
			done = atomic.LoadPointer(&n.done)
		}
	}

	if done == closedChan {
		n.release()
		return n.next, nil
	}

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-closed:
		return nil, eventstream.ErrCursorClosed
	case <-*(*chan struct{})(done):
		n.release()
		return n.next, nil
	}
}

// link sets n.next and wakes any blocked calls to advance().
func (n *node) link(next *node) {
	n.next = next

	ptr := atomic.SwapPointer(&n.done, closedChan)

	if ptr != nil && ptr != closedChan {
		ch := *(*chan struct{})(ptr)
		close(ch)
	}
}

// Less returns true if n contains events before v. It is implemented to satisfy
// the pqueue.Elem interface so that nodes can be used on the reorder queue.
func (n *node) Less(v pqueue.Elem) bool {
	return n.begin < v.(*node).begin
}

// closedChan is a pointer to a "pre-closed done channel". It is used as a sentinel
// value to avoid waiting on the channel if it's known to be closed.
var closedChan = unsafe.Pointer(&pooling.ClosedDoneChan)
