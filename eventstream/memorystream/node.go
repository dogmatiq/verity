package memorystream

import (
	"sync/atomic"
	"unsafe"

	"github.com/dogmatiq/infix/eventstream"
)

// node is a member of a singly-linked list used to store historical events in
// memory.
type node struct {
	offset   uint64            // immutable, can be read at any time
	resolved unsafe.Pointer    // atomic (*chan struct{}), closed when resolved
	event    eventstream.Event // guarded by resolved channel
	next     *node             // guarded by resolved channel
}

// ready returns a channel that is closed when n.event and n.next have been
// populated.
//
// It returns nil if the values are ready now. This value MUST be checked
// because a read from a nil channel blocks forever.
func (n *node) ready() <-chan struct{} {
	ptr := atomic.LoadPointer(&n.resolved)

	if ptr == nil {
		ch := make(chan struct{})
		ptr = unsafe.Pointer(&ch)

		if !atomic.CompareAndSwapPointer(&n.resolved, nil, ptr) {
			// CODE COVERAGE: Another goroutine managed to initialize the
			// channel since we called LoadPointer(), which is hard to time
			// correctly in tests.
			//
			// Note, we have to use the existing channel otherwise at least
			// one reader will go un-notified when resolve() is called.
			ptr = atomic.LoadPointer(&n.resolved)
		}
	}

	if ptr == closedChan {
		return nil
	}

	return *(*chan struct{})(ptr)
}

// resolve populates n.event and n.next and closes the "resolved" channel.
func (n *node) resolve(ev eventstream.Event) {
	n.event = ev
	n.next = &node{offset: n.offset + 1}

	ptr := atomic.SwapPointer(&n.resolved, closedChan)

	if ptr != nil && ptr != closedChan {
		ch := *(*chan struct{})(ptr)
		close(ch)
	}
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
