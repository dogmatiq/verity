package memorystream

import (
	"context"
	"sync"
	"sync/atomic"
	"unsafe"

	"github.com/dogmatiq/configkit"
	"github.com/dogmatiq/configkit/message"
	"github.com/dogmatiq/infix/eventstream"
	"github.com/dogmatiq/infix/parcel"
	"github.com/dogmatiq/kyu"
)

// DefaultBufferSize is the default number of recent events to buffer in memory.
const DefaultBufferSize = 100

// Stream is an implementation of eventstream.Stream that reads events from in
// an in-memory buffer.
//
// It is primarily intended as a cache of the most recent events from an
// application's event repository. Although the implementation details reflect
// and favor that use case, it is a well-behaved stream implementation that can
// also be used for testing and prototyping.
//
// The stream is "self truncating", dropping the oldest events when the size
// exceeds a pre-defined limit.
//
// The stream's buffer is implemented as an append-only singly-linked list. Each
// node in the list contains a single event. A linked list is used to allow the
// oldest events to be truncated from the buffer without invalidating cursors
// that may still be iterating through those events. Coupled with the use of
// atomic operations for reading the head and tail of the linked list, the
// stream's cursor implementation is lock-free.
type Stream struct {
	// App is the identity of the application that owns the stream.
	App configkit.Identity

	// Types is the set of supported event types.
	Types message.TypeCollection

	// FirstOffset is the first offset that will be kept in this stream.
	//
	// This value is used to quickly reject Open() calls that ask for events
	// that will never be buffered, rather than waiting until they timeout, or
	// worse yet blocking forever if their context has no deadline.
	//
	// When the stream is used as a recent event cache, this should be set to
	// the next unused offset in the event repository before new events are
	// allowed to be produced.
	FirstOffset uint64

	// BufferSize is the maximum number of messages to buffer in memory. If it
	// is non-positive, DefaultBufferSize is used.
	//
	// When the number of buffered events exceeds this limit, the oldest nodes
	// in the buffer are truncated until the size falls below the limit again.
	BufferSize int

	m          sync.Mutex
	head, tail unsafe.Pointer // atomic (*node), guarded by m for writes only in order to keep 'size' accurate
	reorder    kyu.PQueue     // priority queue of events that arrive out of order
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

	// Load the tail of the linked-list first to see if the requested offset is
	// the next expected offset. This should be the common case when used as
	// recent event cache because the memory stream is only queried when no more
	// events can be loaded from the event repository.
	n := s.loadTail()

	if n == nil {
		// If the tail is nil then the list is uninitialized. We initialize it
		// with a node that will contain the as-yet-unknown event at
		// c.FirstOffset.
		s.m.Lock()
		n = s.init()
		s.m.Unlock()
	} else if o < n.offset {
		// We already have an initialized list, and the requested offset is for
		// a historical event. Starting at the head, the cursor will scan
		// through the list nodes until it finds the desired offset.
		n = s.loadHead()
	}

	return &cursor{
		offset: o,
		filter: f,
		node:   n,
		closed: make(chan struct{}),
	}, nil
}

// Append adds events to the tail of the stream.
func (s *Stream) Append(parcels ...parcel.Parcel) {
	s.m.Lock()

	tail := s.loadTail()
	offset := s.FirstOffset
	if tail != nil {
		offset = tail.offset
	}

	for _, p := range parcels {
		s.grow(eventstream.Event{
			Offset: offset,
			Parcel: p,
		})
		offset++
	}

	s.shrink()

	s.m.Unlock()
}

// Add events to the stream's buffer.
//
// The events do not necessarily have to be in order. Any out of order events
// are added to the "reordering" queue until sufficient events have been added
// to close the "gap", at which point they are made visible to open cursors.
func (s *Stream) Add(events []eventstream.Event) {
	s.m.Lock()

	for _, ev := range events {
		s.grow(ev)
	}

	s.shrink()

	s.m.Unlock()
}

// grow adds an event to the buffer or the reordering queue, as appropriate.
// It assumes s.m is already locked.
func (s *Stream) grow(ev eventstream.Event) {
	tail := s.loadTail()
	if tail == nil {
		tail = s.init()
	}

	if ev.Offset < tail.offset {
		// This event is earlier than what we're expecting next so we just
		// discard it. This "should never happen" provided that s.FirstOffset is
		// populated before new events are allowed to be produced when the
		// engine first starts.
		return
	}

	// Regardless of whether this event needs to be reordered or not we include
	// it in the size for the purpose of enforcing the size limit.
	s.size++

	if ev.Offset > tail.offset {
		// This events has arrived out of order, we keep it in the reorder queue
		// until we are given the event that immediately preceeds it.
		s.reorder.Push(ev)
		return
	}

	// We were given the event we expected next. We resolve the tail node with
	// ev, and continue to do so using events from the reordering queue so long
	// as we have a contiguous series of offsets.
	for {
		tail.resolve(ev)
		tail = tail.next
		s.storeTail(tail)

		e, ok := s.reorder.Peek()
		if !ok {
			return
		}

		ev = e.Value.(eventstream.Event)
		if ev.Offset != tail.offset {
			return
		}

		s.reorder.Pop()
	}
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
	// reach the tail.
	//
	// We can read head.next here without using node.ready() because s.m is
	// still locked, and any node.resolve() calls have already been made.
	for s.size > limit && head.next != nil {
		s.size--
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
		s.reorder.Less = func(a, b interface{}) bool {
			return a.(eventstream.Event).Offset < b.(eventstream.Event).Offset
		}

		n = &node{offset: s.FirstOffset}
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
