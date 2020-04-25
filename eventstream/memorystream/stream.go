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
	tail    *node
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

	n := s.loadHead()

	if n == nil {
		s.m.Lock()
		s.init()
		s.m.Unlock()

		n = s.loadHead()
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
	}

	s.m.Lock()

	s.init()
	s.grow(n)
	s.shrink()

	s.m.Unlock()
}

// grow adds the node to the buffer, advancing s.tail if possible.
func (s *Stream) grow(n *node) {
	if n.begin < s.tail.end {
		// These events are earlier than what we're expecting next so we just
		// discard them. This "should never happen" provided that b.NextOffset
		// is populated before new events are allowed to be produced when the
		// engine first starts.
		return
	}

	// Regardless of whether these events need to be reordered we include them
	// in the size for the purpose of enforcing the size limit.
	s.size += len(n.parcels)

	if n.begin > s.tail.end {
		// These events has arrived out of order, keep the node in the reorder
		// queue until it can be linked to its preceeding node.
		s.reorder.Push(n)
		return
	}

	// This node is the one we're expecting next. We link any out-of-order nodes
	// that follow on from this one before linking n to s.tail. This ensures any
	// reads that pass through these nodes will always take the fast-path of
	// node.advance().
	tail := n
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

	// Finally, we link s.tail.next with n and replace it with the new tail,
	// waking any reads that had hit the tail.
	s.tail.link(n)
	s.tail = tail
}

// shrink advances b.head until s.size is below the buffer size.
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
		head = head.next
	}

	// Replace the old head with the new one.
	s.storeHead(head)
}

// init sets up b.head and s.tail with an initial empty batch based on
// b.NextOffset.
func (s *Stream) init() {
	if s.tail != nil {
		return
	}

	s.tail = &node{
		begin: s.FirstOffset,
		end:   s.FirstOffset,
	}

	s.storeHead(s.tail)
}

// loadHead atomically loads the node pointed to by b.head.
func (s *Stream) loadHead() *node {
	return (*node)(atomic.LoadPointer(&s.head))
}

// storeHead atomically updates b.head to point to *n.
func (s *Stream) storeHead(n *node) {
	atomic.StorePointer(&s.head, unsafe.Pointer(n))
}
