package memorystream

import (
	"context"
	"sync"
	"sync/atomic"
	"unsafe"

	"github.com/dogmatiq/configkit"
	"github.com/dogmatiq/configkit/message"
	"github.com/dogmatiq/infix/eventstream"
	"github.com/dogmatiq/infix/internal/x/containerx/pqueue"
	"github.com/dogmatiq/infix/parcel"
)

// DefaultBufferSize is the default number of recent events to buffer in memory.
const DefaultBufferSize = 100

// Stream is an in-memory stream.
type Stream struct {
	// App is the identity of the application that owns the stream.
	App configkit.Identity

	// Types is the set of supported event types.
	Types message.TypeCollection

	// FirstOffset is the first offset that will be kept in this stream.
	// This is used to determine whether Open() should return ErrTruncated.
	FirstOffset uint64

	// BufferSize is the maximum number of messages to buffer in memory.
	// If it is non-positive, DefaultBufferSize is used.
	BufferSize int

	m       sync.Mutex
	reorder pqueue.Queue
	tail    unsafe.Pointer // atomic (*node)
	head    unsafe.Pointer // atomic (*node)
	size    int
}

// Application returns the identity of the application that owns the stream.
func (s *Stream) Application() configkit.Identity {
	return s.App
}

// EventTypes returns the set of event types that may appear on the stream.
func (s *Stream) EventTypes(context.Context) (message.TypeCollection, error) {
	return s.Types, nil
}

// Open returns a cursor that reads events from the stream.
//
// o is the offset of the first event to read. The first event on a stream
// is always at offset 0.
//
// f is the set of "filter" event types to be returned by Cursor.Next(). Any
// other event types are ignored.
//
// It returns an error if any of the event types in f are not supported, as
// indicated by EventTypes().
func (s *Stream) Open(
	ctx context.Context,
	o uint64,
	f message.TypeCollection,
) (eventstream.Cursor, error) {
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}

	if f.Len() == 0 {
		panic("at least one event type must be specified")
	}

	// Load the tail first to see if the requested offset is at or after the
	// batch within. If it is, we don't have to step through all of the nodes in
	// the list to find out offset.
	n := s.loadTail()

	if n == nil {
		// If the node is nil then the list is uninitialized. Initialize it with
		// an empty node and begin waiting.
		s.m.Lock()
		n = s.init()
		s.m.Unlock()
	}

	for !n.acquire() {
		// CODE COVERAGE: The tail node we just loaded has been invalidated,
		// which is highly unlikely. In order for this branch to be executed
		// s.tail to have been updated *and* n became s.head at some point *and*
		// it was truncated from the buffer while it was at s.head, all since we
		// called s.loadTail() the last time.
		n = s.loadTail()
	}

	if o < n.begin {
		// Our requested offset is NOT at or after the tail node, so now we have
		// to load the head and step through the list until we find the node
		// that contains the offset we want.

		n.release() // release the tail node

		n = s.loadHead()
		for !n.acquire() {
			// CODE COVERAGE: The head node we *just* loaded has been
			// invalidated and replaced with a new head node. This hard to time
			// correctly in a test, but likely to occur in production.
			n = s.loadHead()
		}
	}

	if o < n.begin {
		return nil, eventstream.ErrTruncated
	}

	return &cursor{
		offset: o,
		filter: f,
		head:   n,
		closed: make(chan struct{}),
	}, nil
}

// Add adds events to the stream. This "batch" of events may be out of order
// from those events already in the stream.
func (s *Stream) Add(
	begin uint64,
	parcels []*parcel.Parcel,
) {
	n := &node{
		begin:   begin,
		end:     begin + uint64(len(parcels)),
		parcels: parcels,
		refs:    1, // hold a ref for the stream itself
	}

	s.m.Lock()
	s.grow(n)
	s.shrink()
	s.m.Unlock()
}

// grow adds the node to the buffer, advancing s.tail if possible.
func (s *Stream) grow(n *node) {
	tail := s.loadTail()
	if tail == nil {
		tail = s.init()
	}

	if n.begin < tail.end {
		// These events are earlier than what we're expecting next so we just
		// discard them. This "should never happen" provided that b.NextOffset
		// is populated before new events are allowed to be produced when the
		// engine first starts.
		return
	}

	// Regardless of whether these events need to be reordered we include them
	// in the size for the purpose of enforcing the size limit.
	s.size += len(n.parcels)

	if n.begin > tail.end {
		// These events has arrived out of order, keep the node in the reorder
		// queue until it can be linked to its preceeding node.
		s.reorder.Push(n)
		return
	}

	// This node is the one we're expecting next. We link any out-of-order nodes
	// that follow on from this one before linking n to s.tail. This ensures any
	// reads that pass through these nodes will always take the fast-path of
	// node.advance().
	orig := tail
	tail = n
	for {
		if x, ok := s.reorder.Peek(); ok {
			x := x.(*node)
			if x.begin == tail.end {
				s.reorder.Pop()
				tail.link(x)
				tail = x
				continue
			}
		}

		break
	}

	// Finally, we link the chain of nodes we've just made with the original
	// s.tail, waking any reads that had hit the tail.
	orig.link(n)
	s.storeTail(tail)
}

// shrink advances s.head until s.size is below the buffer size.
func (s *Stream) shrink() {
	limit := s.BufferSize
	if limit <= 0 {
		limit = DefaultBufferSize
	}

	if s.size < limit {
		return
	}

	// Load the current head.
	head := s.loadHead()

	// Continue to replace head with head.next until we're under the limit or we
	// reach the tail. We can read head.next here without using Batch.Next()
	// because we still hold the lock on b.m.
	for s.size > limit && head.next != nil {
		s.size -= len(head.parcels)

		// Replace the old head with the new one.
		s.storeHead(head.next)
		head.release()
		head = head.next
	}
}

// init sets up s.head and s.tail with an initial empty batch based on
// b.NextOffset.
//
// It assumes s.m is already locked.
func (s *Stream) init() *node {
	n := s.loadHead()

	if n == nil {
		n = &node{
			begin: s.FirstOffset,
			end:   s.FirstOffset,
			refs:  1, // hold a ref for the stream itself
		}

		s.storeHead(n)
		s.storeTail(n)
	}

	return n
}

// loadHead atomically loads the node pointed to by s.head.
func (s *Stream) loadHead() *node {
	return (*node)(atomic.LoadPointer(&s.head))
}

// storeHead atomically updates s.head to point to *n.
func (s *Stream) storeHead(n *node) {
	atomic.StorePointer(&s.head, unsafe.Pointer(n))
}

// loadTail atomically loads the node pointed to by s.tail.
func (s *Stream) loadTail() *node {
	return (*node)(atomic.LoadPointer(&s.tail))
}

// storeTail atomically updates s.tail to point to *n.
func (s *Stream) storeTail(n *node) {
	atomic.StorePointer(&s.tail, unsafe.Pointer(n))
}
