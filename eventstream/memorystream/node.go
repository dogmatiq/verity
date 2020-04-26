package memorystream

import (
	"context"
	"math"
	"sync"
	"sync/atomic"

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

	done uint32 // atomic bool, if non-zero then next is non-nil
	refs int32  // atomic
	next *node
	once sync.Once
	wake chan struct{}
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
		pooling.ParcelSlice.Put(n.parcels)
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
	if atomic.LoadUint32(&n.done) != 0 {
		n.release()
		return n.next, nil
	}

	n.once.Do(func() {
		n.wake = make(chan struct{})
	})

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-closed:
		return nil, eventstream.ErrCursorClosed
	case <-n.wake:
		n.release()
		return n.next, nil
	}
}

// link sets n.next and wakes any blocked calls to advance().
func (n *node) link(next *node) {
	n.next = next
	atomic.StoreUint32(&n.done, 1)

	n.once.Do(func() {
		n.wake = closed
	})

	if n.wake == closed {
		return
	}

	close(n.wake)
}

// Less returns true if n contains events before v. It is implemented to satisfy
// the pqueue.Elem interface so that nodes can be used on the reorder queue.
func (n *node) Less(v pqueue.Elem) bool {
	return n.begin < v.(*node).begin
}

// closed is a pre-closed channel used instead of creating a new "wake" channel
// whenever Batch.link() is called before there were any blocked calls to
// Next().
var closed chan struct{}

func init() {
	closed = make(chan struct{})
	close(closed)
}
