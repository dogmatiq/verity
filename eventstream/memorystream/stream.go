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

// Stream is an implementation of eventstream.Stream that reads events from in
// an in-memory buffer.
//
// It is primarily intended as a cache of the most recent events from an
// application's event store. The implementation details reflect and favor that
// use case, though it is a well-behaved stream implementation that can also be
// used for testing and prototyping.
//
// The stream is "self truncating", dropping the oldest events when the size
// exceeds some pre-defined limit.
//
// The stream's buffer is implemented as a singly-linked list. Each node in the
// list contains a "batch" of events that were added by a single call to Add().
// When used as event store cache, each batch correlates to the events produced
// within a single transaction (which is most likely a single event).
//
// This approach allows the oldest events to be truncated without invalidating
// the list nodes, which in turn allows the stream's cursor to be lock-free.
type Stream struct {
	// App is the identity of the application that owns the stream.
	App configkit.Identity

	// Types is the set of supported event types.
	Types message.TypeCollection

	// FirstOffset is the first offset that will be kept in this stream.
	//
	// This value is used to quickly reject Open() calls that ask for events
	// that will never be buffered, rather than waiting until they timeout, or
	// worse yet block forever if their context has no deadline.
	//
	// When used an event store cache this should be set to the next unused
	// offset in the event store before new events are allowed to be produced.
	FirstOffset uint64

	// BufferSize is the maximum number of messages to buffer in memory. If it
	// is non-positive, DefaultBufferSize is used.
	//
	// When the number of buffered events exceeds this limit, the oldest nodes
	// in the buffer are truncated until the size falls below the limit again.
	//
	// The number of events in memory will not necessarily be maintained at or
	// near this limit. Old events are truncated in batches, which could
	// theoretically contain a large proportion of the buffered events.
	// Additionaly, the tail node is never truncated.
	BufferSize int

	m          sync.Mutex
	head, tail unsafe.Pointer // atomic (*node), guarded by m for writes only in order to keep 'size' accurate
	reorder    pqueue.Queue   // priority queue of batches that arrive out of order
	size       int            // total number of buffered events, including those in the reorder queue
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
// o is the offset of the first event to read. The first event on a stream is
// always at offset 0.
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

	if o < s.FirstOffset {
		// Fail fast if this offset will never appear in the buffer.
		return nil, eventstream.ErrTruncated
	}

	// Load the tail of the linked-list first to see if the requested offset is
	// at or after the batch within. This should be the common case when used as
	// event store cache because the memory stream is only queried when no more
	// events can be loaded from the persistence store.
	n := s.loadTail()

	if n == nil {
		// If the tail is nil then the list is uninitialized. We initialize it
		// to a single node containing an empty batch of events.
		s.m.Lock()
		n = s.init()
		s.m.Unlock()
	} else if o < n.begin {
		// We already have an initialized list, and the requested offset is not
		// within or after the tail node, so have to scan the entire list
		// starting at the head.
		n = s.loadHead()

		if o < n.begin {
			// The requested offset is even older than the head of the list,
			// meaning these events have already been truncated.
			return nil, eventstream.ErrTruncated
		}
	}

	return &cursor{
		offset: o,
		filter: f,
		node:   n,
		closed: make(chan struct{}),
	}, nil
}

// Add adds a batch of events to the stream's buffer.
//
// begin is the offset of the first event in the batch.
//
// Batches do not need to be added in order. If begin does not follow directly
// from the most recent event already in the buffer the batch is added to a
// "reordering" queue and only made available to cursors when the "gap" has been
// closed.
func (s *Stream) Add(
	begin uint64,
	parcels []*parcel.Parcel,
) {
	n := &node{
		begin:   begin,
		end:     begin + uint64(len(parcels)),
		parcels: parcels,
	}

	s.m.Lock()
	s.grow(n)
	s.shrink()
	s.m.Unlock()
}

// grow adds a node to the buffer or the reordering queue, as appropriate.
//
// It assumes s.m is already locked.
func (s *Stream) grow(n *node) {
	tail := s.loadTail()
	if tail == nil {
		tail = s.init()
	}

	if n.begin < tail.end {
		// These events are earlier than what we're expecting next so we just
		// discard them. This "should never happen" provided that s.FirstOffset
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

	// These events are the ones we were expecting next. First, we we make the
	// longest chain possible using the nodes from the reorderer queue before
	// linking s.tail to n. This ensures any cursor reads that pass through
	// these nodes will always take the fast-path of node.advance(), at the cost
	// of holding the mutex lock a little longer.
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
	// s.tail, waking any cursors that had hit the tail.
	orig.link(n)
	s.storeTail(tail)
}

// shrink advances s.head until s.size is below the buffer size.
//
// It assumes s.m is already locked.
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
	// reach the tail. We can read head.next here without using node.next()
	// because we still hold the lock on the mutex any node.link() calls have
	// already been made for this call to Add().
	for s.size > limit && head.next != nil {
		s.size -= len(head.parcels)
		head = head.next
	}

	// Replace the old head with the new one.
	s.storeHead(head)
}

// init sets up the linked-list to contain a single empty node.
//
// It assumes s.m is already locked.
func (s *Stream) init() *node {
	n := s.loadHead()

	if n == nil {
		n = &node{
			begin: s.FirstOffset,
			end:   s.FirstOffset,
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
