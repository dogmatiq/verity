package queue

import (
	"context"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/dogmatiq/dodeca/logging"
	"github.com/dogmatiq/infix/internal/x/containerx/pdeque"
	"github.com/dogmatiq/infix/parcel"
	"github.com/dogmatiq/infix/persistence"
	"github.com/dogmatiq/infix/persistence/subsystem/queuestore"
	"github.com/dogmatiq/marshalkit"
)

// DefaultBufferSize is the default size of the in-memory queue buffer.
var DefaultBufferSize = runtime.GOMAXPROCS(0) * 10

// A Queue is an prioritized collection of messages.
//
// It exposes an application's message queue to multiple consumers, ensuring
// each consumer receives a different message.
//
// It implements the dogma.CommandExecutor interface.
type Queue struct {
	// DataStore is the data-store that stores the queued messages.
	//
	// It is expected that no messages will be saved to the queue for this
	// data-store, other than via this Queue.
	DataStore persistence.DataStore

	// Marshaler is used to unmarshal messages read from the queue store.
	Marshaler marshalkit.ValueMarshaler

	// BufferSize is the maximum number of messages to buffer in memory.
	// If it is non-positive, DefaultBufferSize is used.
	//
	// It should be larger than the number of concurrent consumers.
	BufferSize int

	// Logger is the target for log messages from the queue.
	// If it is nil, logging.DefaultLogger is used.
	Logger logging.Logger

	// A "tracked" message is that is being managed by this Queue. All tracked
	// messages are already persisted in the queue store.
	//
	// The tracked messages are always those with the highest-priority, that is,
	// those that are scheduled to be handled the soonest.
	//
	// Every tracked message is either being handled now (within a Request), or
	// it's in the "pending" queue.
	tracked    map[string]struct{} // key == message ID
	pending    pdeque.Deque        // priority queue of messages without active requests
	exhaustive uint32              // atomic tri-bool, see exhaustiveXXX consts.

	once sync.Once
	done chan struct{} // closed when Run() exits
	in   chan *elem    // delivers elements to Run() for tracking
	out  chan *elem    // delivers elements to Pop() for handling
}

const (
	// exhaustiveUnknown means that there may or may not be persisted messages
	// that are untracked.
	exhaustiveUnknown uint32 = iota

	// exhaustiveYes means that all persisted messages are tracked.
	exhaustiveYes

	// exhaustiveNo means that there are untracked persisted messages.
	exhaustiveNo
)

// elem is a message that is tracked by a Queue.
//
// It implements the pdeque.Elem interface.
type elem struct {
	parcel  *parcel.Parcel
	item    *queuestore.Item
	tracked bool // true once elem is actually in the tracked list
}

func (e *elem) Less(v pdeque.Elem) bool {
	return e.item.NextAttemptAt.Before(
		v.(*elem).item.NextAttemptAt,
	)
}

// Pop returns a request for the message popped from the front of the queue.
//
// It blocks until a message is ready to be handled or ctx is canceled.
func (q *Queue) Pop(ctx context.Context) (*Request, error) {
	q.init()

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case e := <-q.out:
		return &Request{
			queue: q,
			elem:  e,
		}, nil
	}
}

// Track begins tracking a message that has been persisted to the queue store.
//
// TODO: accept a slice.
func (q *Queue) Track(m Message) {
	if m.Item.Revision == 0 {
		panic("message must be persisted")
	}

	e := &elem{
		parcel: m.Parcel,
		item:   m.Item,
	}

	q.init()

	// Now that we've persisted the message, we need to start tracking it.
	select {
	case q.in <- e:
		logging.Debug(q.Logger, "%s requested tracking successfully", e.item.ID())

	default:
		// We weren't able to hand off the element for tracking. Set the
		// exhaustive state to NO so that Run() knows to try loading messages
		// from the store again.
		atomic.StoreUint32(&q.exhaustive, exhaustiveNo)
		logging.Debug(q.Logger, "%s requested tracking, but the tracking queue is full (exhaustive: no)", e.item.ID())
	}
}

// Run starts the queue.
//
// It coordinates the tracking of messages that are loaded from the queue store,
// or manually added to the queue by Track().
func (q *Queue) Run(ctx context.Context) error {
	q.init()
	defer close(q.done)

	for {
		if err := q.tick(ctx); err != nil {
			return err
		}
	}
}

// peek returns the element at the front of the queue, loading messages from the
// store if none are currently tracked.
func (q *Queue) peek(ctx context.Context) (*elem, bool, error) {
	if x, ok := q.pending.PeekFront(); ok {
		return x.(*elem), true, nil
	}

	if err := q.load(ctx); err != nil {
		return nil, false, err
	}

	if x, ok := q.pending.PeekFront(); ok {
		return x.(*elem), true, nil
	}

	return nil, false, nil
}

// tick performs one "step" of Run().
func (q *Queue) tick(ctx context.Context) error {
	var (
		// Setup channel variables that we only populate if we want to use
		// them. Otherwise we leave them nil, causing them to block forever,
		// having no effect on the select below.
		out  chan<- *elem
		wait <-chan time.Time
	)

	e, ok, err := q.peek(ctx)
	if err != nil {
		return err
	}

	if ok {
		d := time.Until(e.item.NextAttemptAt)

		if d <= 0 {
			out = q.out
			logging.Debug(q.Logger, "%s is ready to be handled immediately", e.item.ID())
		} else {
			timer := time.NewTimer(d)
			defer timer.Stop()
			wait = timer.C
			logging.Debug(q.Logger, "%s will not be ready for another %s", e.item.ID(), d)
		}
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()

		case <-wait:
			logging.Debug(q.Logger, "%s is now ready to be handled", e.item.ID())
			out = q.out
			wait = nil

		case out <- e:
			q.pending.PopFront()
			logging.Debug(q.Logger, "%s was sent to a consumer", e.item.ID())
			return nil

		case e := <-q.in:
			if q.update(e) {
				return nil
			}
		}
	}
}

// load reads queued messages and starts tracking them.
func (q *Queue) load(ctx context.Context) error {
	if n := len(q.tracked); n != 0 {
		// We can only load the highest-priority messages from the data-store,
		// so if there's anything already tracked that is exactly what we would
		// be loading.
		logging.Debug(q.Logger, "not loading messages, %d message(s) already tracked", n)
		return nil
	}

	if atomic.LoadUint32(&q.exhaustive) == exhaustiveYes {
		// All messages are known to be tracked. The pending queue may be empty
		// right now, but that just means that there are active requests for
		// every message still on the queue or that the queue is truly empty.
		logging.DebugString(q.Logger, "not loading messages (exhaustive: yes)")
		return nil
	}

	// We set the status to UNKNOWN while we are loading. If a call to Track()
	// comes along and sets it to NO in the mean-time, we know not to set it
	// back to YES after we finish loading as we may or may not have that
	// particular message in the result of the load.
	atomic.StoreUint32(&q.exhaustive, exhaustiveUnknown)

	limit := q.BufferSize
	if limit <= 0 {
		limit = DefaultBufferSize
	}

	// Load messages up to our configured limit.
	logging.Debug(q.Logger, "loading up to %d message(s)", limit)
	items, err := q.DataStore.QueueStoreRepository().LoadQueueMessages(ctx, limit)
	if err != nil {
		return err
	}

	n := len(items)

	if n < limit && atomic.CompareAndSwapUint32(
		&q.exhaustive,
		exhaustiveUnknown,
		exhaustiveYes,
	) {
		// We didn't get as many messages as we requested, so we know we have
		// everything in memory.
		//
		// Only set the exhaustive status back to YES if a Track() call did not
		// set it to NO while we were loading.
		logging.Debug(q.Logger, "loaded %d message(s) from store (exhaustive: yes)", n)
	} else {
		logging.Debug(q.Logger, "loaded %d message(s) from store (exhaustive: unknown)", n)
	}

	for _, i := range items {
		q.update(&elem{item: i})
	}

	return nil
}

// update updates the tracked list and pending queue to reflect changes to e.
//
// It returns true if a new peek() should be performed.
func (q *Queue) update(e *elem) bool {
	limit := q.BufferSize

	if limit <= 0 {
		limit = DefaultBufferSize
	}

	if e.item.Revision == 0 {
		// The message has been handled successfully and removed from the queue
		// store, stop tracking it.
		delete(q.tracked, e.item.ID())
		logging.Debug(q.Logger, "%s is no longer tracked, handled successfully (pending: %d, tracked: %d/%d)", e.item.ID(), q.pending.Len(), len(q.tracked), limit)
		return len(q.tracked) == 0
	}

	if e.tracked {
		// The message is already tracked, just put it back in the pending
		// queue to be re-attempted.
		logging.Debug(q.Logger, "%s has been returned to the pending queue (pending: %d, tracked: %d/%d)", e.item.ID(), q.pending.Len(), len(q.tracked), limit)
		return q.pending.Push(e)
	}

	// Start tracking the message.
	before := len(q.tracked)
	q.tracked[e.item.ID()] = struct{}{}
	after := len(q.tracked)

	if before == after {
		// The message was already be in the tracked list, which can occur if it
		// was persisted just before we loaded form the store so we see the
		// message both in the load result and on the q.in channel.
		logging.Debug(q.Logger, "%s is already tracked (pending: %d, tracked: %d/%d)", e.item.ID(), q.pending.Len(), len(q.tracked), limit)
		return false
	}

	e.tracked = true
	head := q.pending.Push(e)

	if after > limit {
		// We're tracking more messages than the limit allows, drop the
		// lowest-priority element.
		drop, _ := q.pending.PopBack()
		delete(q.tracked, drop.(*elem).item.ID())

		// There are now persisted messages that are not in the buffer.
		atomic.StoreUint32(&q.exhaustive, exhaustiveNo)

		if drop == e {
			// This is the message we just pushed. That would mean that all
			// buffered messages are in active requests, which should not occur
			// if BufferSize is larger than the number of consumers as suggested
			// by its documentation.
			logging.Debug(q.Logger, "%s will not be tracked, buffer size limit reached (exhaustive: no, pending: %d, tracked: %d/%d)", e.item.ID(), q.pending.Len(), len(q.tracked), limit)
			return false
		}

		logging.Debug(q.Logger, "%s is no longer tracked, buffer size limit reached (exhaustive: no, pending: %d, tracked: %d/%d)", e.item.ID(), q.pending.Len(), len(q.tracked), limit)
	}

	logging.Debug(q.Logger, "%s is now tracked (pending: %d, tracked: %d/%d)", e.item.ID(), q.pending.Len(), len(q.tracked), limit)
	return head
}

// notify informs the queue that the state of e has changed.
func (q *Queue) notify(e *elem) {
	select {
	case q.in <- e:
		return // keep to see coverage
	case <-q.done:
		return // keep to see coverage
	}
}

// init initializes the queue's internal state.
func (q *Queue) init() {
	q.once.Do(func() {
		limit := q.BufferSize
		if limit <= 0 {
			limit = DefaultBufferSize
		}

		// Allocate capcity for our limit +1 element used to detect overflow.
		q.tracked = make(map[string]struct{}, limit+1)

		// Buffered to avoid blocking in Track(), doesn't strictly *need* to be
		// the same size as limit.
		q.in = make(chan *elem, limit)

		q.done = make(chan struct{})
		q.out = make(chan *elem)
	})
}
