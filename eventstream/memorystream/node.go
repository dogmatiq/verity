package memorystream

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/dogmatiq/infix/eventstream"
	"github.com/dogmatiq/infix/internal/x/containerx/pqueue"
	"github.com/dogmatiq/infix/parcel"
)

// node is a member of a singly-linked list of contiguous "batches" of events.
type node struct {
	begin   uint64
	end     uint64
	parcels []*parcel.Parcel

	done uint32 // atomic bool, if non-zero then next is non-nil
	next *node
	once sync.Once
	wake chan struct{}
}

// advance returns the next node in the list.
// It blocks until the next node is linked or or ctx is canceled.
func (n *node) advance(
	ctx context.Context,
	closed <-chan struct{},
) (*node, error) {
	if atomic.LoadUint32(&n.done) != 0 {
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
