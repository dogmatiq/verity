package memorystream

import (
	"sync/atomic"
	"unsafe"

	"github.com/dogmatiq/infix/internal/x/containerx/pqueue"
	"github.com/dogmatiq/infix/parcel"
)

// node is a member of a singly-linked list of contiguous "batches" of events.
type node struct {
	// the fields below are immutable and may be read at any time.
	begin   uint64
	end     uint64
	parcels []*parcel.Parcel

	linked unsafe.Pointer // atomic (*chan struct{}), closed when next is populated
	next   *node          // the next node in the list
}

// ready returns a channel that is closed when n.next is ready for reading.
//
// It returns nil if n.next is ready now. This value must be checked because a
// read from a nil channel blocks forever.
func (n *node) ready() <-chan struct{} {
	ptr := atomic.LoadPointer(&n.linked)

	if ptr == nil {
		ch := make(chan struct{})
		ptr = unsafe.Pointer(&ch)

		if !atomic.CompareAndSwapPointer(&n.linked, nil, ptr) {
			// CODE COVERAGE: Another goroutine managed to initialize the
			// channel since we called LoadPointer(), which is hard to time
			// correctly in tests.
			//
			// Note, we have to use the the existing channel otherwise at least
			// one reader will go un-notified when link() is called.
			ptr = atomic.LoadPointer(&n.linked)
		}
	}

	if ptr == closedChan {
		return nil
	}

	return *(*chan struct{})(ptr)
}

// link sets n.next and closes the "linked" channel.
func (n *node) link(next *node) {
	n.next = next

	ptr := atomic.SwapPointer(&n.linked, closedChan)

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

// closedChan is a pointer to a "pre-closed channel". It is used to avoid
// creating a new channel just to close it, and as a sentinel value to avoid
// waiting on the channel if it's known to be closed.
var closedChan unsafe.Pointer

func init() {
	ch := make(chan struct{})
	close(ch)
	closedChan = unsafe.Pointer(&ch)
}
