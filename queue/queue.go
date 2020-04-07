package queue

import (
	"context"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/dogmatiq/dodeca/logging"
	"github.com/dogmatiq/infix/envelope"
	"github.com/dogmatiq/infix/internal/x/containerx/pdeque"
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
	// Every tracked message is either being handled now (within a Session), or
	// it's in the "pending" queue.
	tracked    map[string]struct{} // key == message ID
	pending    pdeque.Deque        // priority queue of messages without active sessions
	exhaustive uint32              // atomic tri-bool, see exhaustiveXXX consts.

	once sync.Once
	done chan struct{} // closed when Run() exits
	in   chan *elem    // delivers elements to Run() for tracking
	out  chan *elem    // delivers messages to Pop() for handling
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

// elem is a container for a message that is tracked by a queue.
//
// It keeps the in-memory envelope representation alongside the protocol buffers
// representation to avoid excess marshaling/unmarshaling.
//
// It implements the pdeque.Elem interface.
type elem struct {
	env     *envelope.Envelope
	message *queuestore.Message
	tracked bool // true once elem is actually in the tracked list
}

func (e *elem) Less(v pdeque.Elem) bool {
	return e.message.NextAttemptAt.Before(
		v.(*elem).message.NextAttemptAt,
	)
}

// Pop removes the message at the front of the queue.
//
// It returns a session within which the message is to be handled.
// It blocks until a message is ready to be handled or ctx is canceled.
func (q *Queue) Pop(ctx context.Context) (*Session, error) {
	q.init()

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case e := <-q.out:
		return &Session{
			queue: q,
			elem:  e,
		}, nil
	}
}

// Push adds a message to the queue.
func (q *Queue) Push(ctx context.Context, env *envelope.Envelope) error {
	m := &queuestore.Message{
		NextAttemptAt: time.Now(),
		Envelope:      envelope.MustMarshal(q.Marshaler, env),
	}

	if err := persistence.WithTransaction(
		ctx,
		q.DataStore,
		func(tx persistence.ManagedTransaction) error {
			return tx.SaveMessageToQueue(ctx, m)
		},
	); err != nil {
		return err
	}

	m.Revision++
	e := &elem{env: env, message: m}

	q.init()

	// Now that we've persisted the message, we need to start tracking it.
	select {
	case <-ctx.Done():
		// We were canceled before we could hand off the element for tracking.
		// Set the exhaustive state to NO so that Run() knows to try loading
		// messages from the store again.
		atomic.StoreUint32(&q.exhaustive, exhaustiveNo)

		return ctx.Err()

	case q.in <- e:
		// We successfully handed off the element for tracking.
		return nil

	case <-q.done:
		// Run() has stopped, so nothing will be tracked, but the message has
		// been persisted so from the perspective of the caller the message has
		// been queued successfully.
		return nil
	}
}

// Run starts the queue.
//
// It coordinates the tracking of messages that are loaded from the queue store,
// or added to the queue by Push() or as part of handling another message.
func (q *Queue) Run(ctx context.Context) error {
	q.init()
	defer close(q.done)

	for {
		if err := q.tick(ctx); err != nil {
			return err
		}
	}
}

// tick performs one "step" of Run().
func (q *Queue) tick(ctx context.Context) error {
	e, d, ok := q.peekOrPopIfReady()

	if !ok {
		loaded, err := q.load(ctx)
		if err != nil {
			return err
		}

		if loaded {
			e, d, ok = q.peekOrPopIfReady()
		}
	}

	var (
		// Setup channel variables that we only populate if we want to use
		// them. Otherwise we leave them nil, causing them to block forever,
		// having no effect on the select below.
		out  chan<- *elem
		wait <-chan time.Time
	)

	if ok {
		if d <= 0 {
			out = q.out
		} else {
			timer := time.NewTimer(d)
			defer timer.Stop()
			wait = timer.C
		}
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()

		case <-wait:
			wait = nil
			out = q.out

		case out <- e:
			return nil

		case e := <-q.in:
			if q.update(e) {
				// If message became the head of the queue, return to
				// re-evaluate the timer, etc.
				return nil
			}
		}
	}
}

// peekOrPopIfReady returns the element at the front of the pending queue and
// the duration until its message is ready to be handled.
//
// If the message is ready to be handled now, the element is popped from the
// pending queue, in which case d <= 0.
//
// ok is false if the pending queue is empty.
func (q *Queue) peekOrPopIfReady() (e *elem, d time.Duration, ok bool) {
	x, ok := q.pending.PeekFront()
	if !ok {
		return nil, 0, false
	}

	e = x.(*elem)
	d = time.Until(e.message.NextAttemptAt)

	if d <= 0 {
		q.pending.PopFront()
	}

	return e, d, true
}

// load reads queued messages and starts tracking them.
//
// It returns true if the head of the pending queue changed.
func (q *Queue) load(ctx context.Context) (bool, error) {
	if len(q.tracked) > 0 {
		// We can only load the highest-priority messages from the data-store,
		// so if there's anything already tracked that is exactly what we would
		// be loading.
		return false, nil
	}

	if atomic.LoadUint32(&q.exhaustive) == exhaustiveYes {
		// All messages are known to be tracked. The pending queue may be empty
		// right now, but that just means that there are active sessions for
		// every message still on the queue or that the queue is truly empty.
		return false, nil
	}

	// We set the status to UNKNOWN while we are loading. If a call to Push()
	// comes along and sets it to NO in the mean-time, we know not to set it
	// back to YES after we finish loading as we may or may not have that
	// particular message in the result of the load.
	atomic.StoreUint32(&q.exhaustive, exhaustiveUnknown)

	size := q.BufferSize
	if size <= 0 {
		size = DefaultBufferSize
	}

	// Load messages up to our configured limit.
	messages, err := q.DataStore.QueueStoreRepository().LoadQueueMessages(ctx, size)
	if err != nil {
		return false, err
	}

	if len(messages) < size {
		// We didn't get as many messages as we requested, so we know we have
		// everything in memory.
		//
		// Only set the exhaustive status back to YES if a Push() call did not
		// set it to NO while we were loading.
		atomic.CompareAndSwapUint32(&q.exhaustive, exhaustiveUnknown, exhaustiveYes)
	}

	ok := false
	for _, m := range messages {
		if q.update(&elem{message: m}) {
			ok = true
		}
	}

	return ok, nil
}

// update updates the tracked list and pending queue to reflect changes to e.
//
// It returns true if e became the head of the pending queue.
func (q *Queue) update(e *elem) bool {
	if e.message.Revision == 0 {
		// The message has been handled successfully and removed from the queue
		// store, stop tracking it.
		delete(q.tracked, e.message.ID())
		return false
	}

	if !e.tracked {
		// Start tracking the message.
		//
		// The message may already be in the tracked list if it was pushed just
		// before we loaded form the store so we see the message both in the load
		// result and on the q.in channel.
		before := len(q.tracked)
		q.tracked[e.message.ID()] = struct{}{}
		after := len(q.tracked)

		if before == after {
			return false
		}

		e.tracked = true
	}

	front := q.pending.Push(e)

	size := q.BufferSize
	if size <= 0 {
		size = DefaultBufferSize
	}

	if len(q.tracked) > size {
		// We're tracking more messages than the limit allows, drop the
		// lowest-priority element.
		//
		// Note: There's a chance this is the message we just pushed. That would
		// mean that all buffered messages are in active sessions, which should
		// not occur if BufferSize is larger than the number of consumers as
		// suggested by its documentation.
		drop, _ := q.pending.PopBack()
		delete(q.tracked, drop.(*elem).message.ID())

		// There are now persisted messages that are not in the buffer.
		atomic.StoreUint32(&q.exhaustive, exhaustiveNo)
	}

	return front
}

// notify informs the queue that the state of e has changed.
func (q *Queue) notify(e *elem) {
	select {
	case q.in <- e:
	case <-q.done:
	}
}

// init initializes the queue's internal state.
func (q *Queue) init() {
	q.once.Do(func() {
		size := q.BufferSize
		if size <= 0 {
			size = DefaultBufferSize
		}

		q.tracked = make(map[string]struct{}, size)
		q.done = make(chan struct{})
		q.in = make(chan *elem, size)
		q.out = make(chan *elem)
	})
}
