package memorystream

import (
	"context"
	"sync/atomic"
	"unsafe"

	"github.com/dogmatiq/infix/eventstream"
	"github.com/dogmatiq/infix/internal/x/containerx/pqueue"
	"github.com/dogmatiq/infix/parcel"
)

// node is a member of a singly-linked list of contiguous "batches" of events.
type node struct {
	begin   uint64
	end     uint64
	parcels []*parcel.Parcel

	done unsafe.Pointer // atomic (*chan struct{})
	next *node
}

// advance returns the next node in the list.
// It blocks until the next node is linked or or ctx is canceled.
func (n *node) advance(
	ctx context.Context,
	closed <-chan struct{},
) (*node, error) {
	done := atomic.LoadPointer(&n.done)

	if done == nil {
		// There's no existing done channel, we'll try to set one.
		ch := make(chan struct{})
		done = unsafe.Pointer(&ch)

		if !atomic.CompareAndSwapPointer(&n.done, nil, done) {
			// CODE COVERAGE: Another goroutine managed to initialize the
			// channel since we called LoadPointer(). We have to use the their
			// channel otherwise at least one of us will go un-notified.
			done = atomic.LoadPointer(&n.done)
		}
	}

	if done == closedChan {
		return n.next, nil
	}

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-closed:
		return nil, eventstream.ErrCursorClosed
	case <-*(*chan struct{})(done):
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

// closedChan is a pointer to a "pre-closed done channel". It is used as a
// sentinel value to avoid waiting on the channel if it's known to be closed.
var closedChan unsafe.Pointer

func init() {
	ch := make(chan struct{})
	close(ch)
	closedChan = unsafe.Pointer(&ch)
}
