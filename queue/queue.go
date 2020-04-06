package queue

import (
	"context"
	"runtime"
	"sync"
	"time"

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

	// A "buffered" message is a message persisted on the queue that has been
	// loaded into memory.
	//
	// The buffered messages are always those with the highest-priority, that
	// is, those that are scheduled to be handled the soonest.
	//
	// Every buffered message is either being handled now (within a Session), or
	// it's in the "pending" queue.
	bufferM    sync.Mutex
	buffered   map[string]struct{} // set of all buffered messages, key == message ID
	pending    pdeque.Deque        // priority queue of messages that aren't being handled
	exhaustive bool                // true if all persisted messages are buffered in memory
	loading    bool                // true if some goroutine is already loading messages

	// The "monitor" is a goroutine that is started whenever there's at least
	// one call to Pop() that is blocking.
	monitorM sync.Mutex
	blocked  int           // number of blocked calls to Pop()
	ready    chan *elem    // recv by blocked call to Pop()
	wake     chan struct{} // recv by monitor, send when the head of the pending list changes
	halt     chan struct{} // recv by monitor, closed when the last blocking call to Pop() gives up
	done     chan struct{} // closed when the monitor goroutine actually finishes
}

// elem is a container for a queued message that is buffered in memory.
//
// It keeps the in-memory envelope representation alongside the protocol buffers
// representation to avoid excess marshaling/unmarshaling.
//
// It implements the pdeque.Elem interface.
type elem struct {
	env     *envelope.Envelope
	message *queuestore.Message
}

func (e *elem) Less(v pdeque.Elem) bool {
	return e.message.NextAttemptAt.Before(
		v.(*elem).message.NextAttemptAt,
	)
}

// Pop removes the message at the front of the queue.
//
// It returns a session within which the message is to be handled.
//
// It blocks until a message is ready to be handled or ctx is canceled.
func (q *Queue) Pop(ctx context.Context) (*Session, error) {
	e, err := q.popPending(ctx)
	if err != nil {
		return nil, err
	}

	tx, err := q.DataStore.Begin(ctx)
	if err != nil {
		q.pushPending(false, e)
		return nil, err
	}

	return &Session{
		queue: q,
		tx:    tx,
		elem:  e,
	}, nil
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

	q.pushPending(true, &elem{
		env:     env,
		message: m,
	})

	return nil
}

// peekOrPopIfReady returns the element at the front of the pending list.
//
// Additionally, if that element's message is ready to be handled now, the
// element is popped. In which case d <= 0.
//
// If the pending list is empty, e is nil, d is meaningless and ok is false.
func (q *Queue) peekOrPopIfReady() (e *elem, d time.Duration, ok bool) {
	q.bufferM.Lock()
	defer q.bufferM.Unlock()

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

// popPending pops the element at the front of the pending list.
//
// If the highest-priority message is not yet ready for handling, it blocks
// until it is.
//
// If the there are no buffered messages it loads messages from the data-store.
func (q *Queue) popPending(ctx context.Context) (*elem, error) {
	e, ok, load := q.popPendingOrLockForLoading()
	if ok {
		return e, nil
	}

	if load {
		e, ok, err := q.loadMessages(ctx)
		if ok || err != nil {
			return e, err
		}
	}

	q.startMonitor()
	defer q.stopMonitor()

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case e, _ := <-q.ready:
		return e, nil
	}
}

// pushPending adds e to the pending list.
//
// It wakes the monitoring goroutine if e.message has the highest priority of
// all pending messages.
func (q *Queue) pushPending(new bool, e *elem) {
	q.bufferM.Lock()
	defer q.bufferM.Unlock()

	oversize := false

	// If the message is "new" it's not in the buffer yet.
	if new && q.track(e) {
		size := q.BufferSize
		if size <= 0 {
			size = DefaultBufferSize
		}

		if len(q.buffered) > size {
			// The buffer has exceeded its size limit, we need to discard the
			// lowest priority element.
			oversize = true
		}
	}

	if q.pending.Push(e) {
		q.wakeMonitor()
	}

	if oversize {
		// We've got more messages in memory than the limit allows, drop the
		// lowest-priority element.
		//
		// Note: There's a chance this is the message we just pushed. That would
		// mean that all buffered messages are in active sessions, which should
		// not occur if BufferSize is larger than the number of consumers as
		// suggested by its documentation.
		drop, _ := q.pending.PopBack()
		delete(q.buffered, drop.(*elem).message.ID())

		// There are now persisted messages that are not in the buffer.
		q.exhaustive = false
	}
}

// loadMessages reads queued messages from the data-store into the buffer.
//
// If messages are loaded, and the highest-priority message is ready to be
// handled now, e is an element containing that message and ok is true.
func (q *Queue) loadMessages(ctx context.Context) (e *elem, ok bool, err error) {
	size := q.BufferSize
	if size <= 0 {
		size = DefaultBufferSize
	}

	// Load just enough messages to fill the buffer.
	messages, err := q.DataStore.QueueStoreRepository().LoadQueueMessages(ctx, size)

	q.bufferM.Lock()
	defer q.bufferM.Unlock()

	// Clear the loading flag regardless of whether there was an error.
	q.loading = false

	if err != nil {
		return nil, false, err
	}

	if len(messages) < size {
		// We didn't get enough messages to fill the buffer, so we know we have
		// everything in memory.
		q.exhaustive = true
	}

	wake := false

	for _, m := range messages {
		x := &elem{message: m}

		if !q.track(x) {
			// This element is already in the buffer. It must have been pushed
			// while we were loading messages.
			continue
		}

		if ok || m.NextAttemptAt.After(time.Now()) {
			// We we have already found ourselves a message, or this one is
			// not ready yet. Push this message into the pending list and
			// wake the monitor goroutine.

			if q.pending.Push(x) {
				wake = true
			}
		} else {
			// If the message at the front of the queue is ready now we are going to
			// return it to our caller directly to avoid starting the monitor
			// goroutine.
			e = x
			ok = true
		}
	}

	if wake {
		q.wakeMonitor()
	}

	return e, ok, nil
}

// track adds e to the buffered list, or returns false if it is already present.
//
// It assumes q.bufferM is already locked.
func (q *Queue) track(e *elem) bool {
	id := e.message.ID()

	if q.buffered == nil {
		q.buffered = map[string]struct{}{
			id: struct{}{},
		}

		return true
	}

	before := len(q.buffered)
	q.buffered[id] = struct{}{}
	after := len(q.buffered)

	return before < after
}

// discard removes e from the buffer.
func (q *Queue) discard(e *elem) {
	q.bufferM.Lock()
	defer q.bufferM.Unlock()

	delete(q.buffered, e.message.ID())
}

// popPendingOrLockForLoading pops a message from the front of the pending list
// if it is ready to be handled.
//
// If the message is ready, ok is true.
//
// Otherwise, if load is true, the m.loading flag has been set and the caller
// should attempt to load messages.
func (q *Queue) popPendingOrLockForLoading() (e *elem, ok bool, load bool) {
	q.bufferM.Lock()
	defer q.bufferM.Unlock()

	if x, ok := q.pending.PeekFront(); ok {
		e = x.(*elem)

		if e.message.NextAttemptAt.After(time.Now()) {
			return nil, false, false
		}

		q.pending.PopFront()

		return e, true, false
	}

	if q.exhaustive {
		// All messages are known to be buffered in memory. The pending list may
		// be empty right now, but that just means that there are active
		// sessions for every message still on the queue or that the queue is
		// truly empty.
		return nil, false, false
	}

	if q.loading {
		// Another goroutine is already loading messages. We don't want to block
		// waiting for it specifically, because we might get a message from a
		// rolled-back session first.
		return nil, false, false
	}

	if len(q.buffered) > 0 {
		// We can only load the highest-priority messages from the data-store,
		// so if there's anything in the buffer at all they will just collide.
		return nil, false, false
	}

	q.loading = true

	return nil, false, true
}

// startMonitor starts the monitoring goroutine if it's not already running.
func (q *Queue) startMonitor() {
	q.monitorM.Lock()
	defer q.monitorM.Unlock()

	if q.ready == nil {
		q.ready = make(chan *elem)
	}

	if q.blocked == 0 {
		q.halt = make(chan struct{})
		q.wake = make(chan struct{}, 1)
		q.done = make(chan struct{})

		go q.monitor()
	}

	q.blocked++
}

// stopMonitor stops the monitoring goroutine if there are no more blocking
// Pop() calls.
func (q *Queue) stopMonitor() {
	q.monitorM.Lock()
	defer q.monitorM.Unlock()

	q.blocked--

	if q.blocked == 0 {
		close(q.halt)
		<-q.done

		q.halt = nil
		q.wake = nil
		q.done = nil
	}
}

// wakeMonitor signals the monitoring goroutine to check the pending list again.
func (q *Queue) wakeMonitor() {
	select {
	case q.wake <- struct{}{}:
	default:
	}
}

// monitor watches the pending list in order to send a message via q.ready when
// the message at the front becomes ready for handling.
func (q *Queue) monitor() {
	defer func() {
		close(q.done)
	}()

	for {
		if !q.deliver() {
			return
		}
	}
}

// deliver blocks until the message at the head of the queue is ready to be
// handled, then sends it via q.ready in order to wake a blocked Pop() call.
//
// It returns false if q.halt is closed.
func (q *Queue) deliver() bool {
	var (
		ready   chan<- *elem
		elapsed <-chan time.Time
	)

	e, d, exists := q.peekOrPopIfReady()

	if exists {
		if d <= 0 {
			ready = q.ready
		} else {
			timer := time.NewTimer(d)
			defer timer.Stop()
			elapsed = timer.C
		}
	}

	select {
	case ready <- e: // ready is nil (and will block forever) if e not ready now
		return true
	case <-elapsed: // elapses is nil (and will block forever) if e is ready now
		return true
	case <-q.wake:
		return true
	case <-q.halt:
		return false
	}
}
