package queue

import (
	"context"
	"runtime"
	"sync"
	"time"

	"github.com/dogmatiq/infix/envelope"
	"github.com/dogmatiq/infix/internal/x/deque"
	"github.com/dogmatiq/infix/persistence"
	"github.com/dogmatiq/infix/persistence/subsystem/queuestore"
	"github.com/dogmatiq/marshalkit"
)

// DefaultBufferSize is the default size of the in-memory queue buffer.
var DefaultBufferSize = runtime.GOMAXPROCS(0) * 10

// A Queue is an prioritized collection of messages.
//
// It implements the dogma.CommandExecutor interface.
type Queue struct {
	// DataStore is the data-store that stores the queued messages.
	DataStore persistence.DataStore

	// Marshaler is used to unmarshal messages read from the queue store.
	Marshaler marshalkit.ValueMarshaler

	// BufferSize is the maximum number of messages to buffer in memory.
	// If it is non-positive, DefaultBufferSize is used.
	BufferSize int

	bufferM sync.Mutex
	buffer  deque.Deque

	deliverM sync.Mutex
	refs     int
	deliver  chan *elem
	reset    chan struct{}
	halt     chan struct{}
	done     chan struct{}
}

// Pop removes the first message from the queue.
//
// It blocks until a message is ready to be handled or ctx is canceled.
// It returns a session within which the message is to be handled.
func (q *Queue) Pop(ctx context.Context) (_ *Session, err error) {
	e, ready, _ := q.peekAndPopIfReady()

	// We didn't get an element, so wait until one becomes ready.
	if !ready {
		// Avoid starting the deliverer goroutine only to stop it again
		// immediately.
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}

		q.start()
		defer q.stop()

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case e = <-q.deliver:
			break // keep to see coverage
		}
	}

	// Return e to the buffer if it's not returned to the caller.
	defer func() {
		if err != nil {
			q.push(e)
		}
	}()

	tx, err := q.DataStore.Begin(ctx)
	if err != nil {
		return nil, err
	}

	return &Session{
		tx:   tx,
		elem: e,
	}, nil
}

// Push adds a message to the queue.
func (q *Queue) Push(ctx context.Context, env *envelope.Envelope) error {
	e := &elem{
		env: env,
		message: &queuestore.Message{
			NextAttemptAt: time.Now(),
			Envelope:      envelope.MustMarshal(q.Marshaler, env),
		},
	}

	return q.save(ctx, e)
}

// save persists a message and adds it to the buffer.
func (q *Queue) save(ctx context.Context, e *elem) error {
	err := persistence.WithTransaction(
		ctx,
		q.DataStore,
		func(tx persistence.ManagedTransaction) error {
			return tx.SaveMessageToQueue(ctx, e.message)
		},
	)

	if err == nil {
		q.push(e)
	}

	return err
}

// push adds e to the buffer, and wakes the deliverer if q becomes the highest
// priority message.
func (q *Queue) push(e *elem) {
	q.bufferM.Lock()
	defer q.bufferM.Unlock()

	if q.buffer.Push(e) {
		// e is now at the front of the queue, try to reset the deliverer.
		select {
		case q.reset <- struct{}{}:
		default:
		}
	}

	size := q.BufferSize
	if size <= 0 {
		size = DefaultBufferSize
	}

	if q.buffer.Len() > size {
		// The buffer is oversized, drop the lowest priority element.
		q.buffer.PopBack()
	}
}

// peekAndPopIfReady returns the first element in the queue, popping it if it is
// ready to be handled now.
//
// If the queue is empty, ok is false. If the message was popped r is true.
func (q *Queue) peekAndPopIfReady() (e *elem, r bool, ok bool) {
	q.bufferM.Lock()
	defer q.bufferM.Unlock()

	x, ok := q.buffer.PeekFront()
	if !ok {
		return nil, false, false
	}

	e = x.(*elem)
	now := time.Now()

	if e.message.NextAttemptAt.After(now) {
		return e, false, true
	}

	q.buffer.PopFront()

	return e, true, true
}

// start runs the deliverer, if it is not already running.
func (q *Queue) start() {
	q.deliverM.Lock()
	defer q.deliverM.Unlock()

	if q.deliver == nil {
		q.deliver = make(chan *elem)
	}

	if q.refs == 0 {
		q.halt = make(chan struct{})
		q.reset = make(chan struct{}, 1)
		q.done = make(chan struct{})

		go q.deliverMany()
	}

	q.refs++
}

// stop halts the deliverer, if there are not more pending Acquire() calls.
func (q *Queue) stop() {
	q.deliverM.Lock()
	defer q.deliverM.Unlock()

	q.refs--

	if q.refs == 0 {
		close(q.halt)
		<-q.done

		q.halt = nil
		q.reset = nil
		q.done = nil
	}
}

// deliverMany repeatedly attempts to deliver the message at the front of the
// queue to a waiting Acquire() call.
func (q *Queue) deliverMany() {
	defer func() {
		close(q.done)
	}()

	for {
		if !q.deliverOne() {
			return
		}
	}
}

// deliverOne attempts to deliver the message at the front of the queue to a
// waiting Acquire() call.
//
// It returns false if q.halt is closed.
func (q *Queue) deliverOne() bool {
	var (
		deliver chan<- *elem
		elapsed <-chan time.Time
	)

	e, ready, ok := q.peekAndPopIfReady()

	if ok {
		if ready {
			deliver = q.deliver
		} else {
			timer := time.NewTimer(
				time.Until(e.message.NextAttemptAt),
			)
			defer timer.Stop()

			elapsed = timer.C
		}
	}

	select {
	case deliver <- e: // nil if not ready
		return true
	case <-elapsed: // nil if no timer
		return true
	case <-q.reset:
		return true
	case <-q.halt:
		return false
	}
}

// elem is a container for a queued message that is buffered in memory.
//
// It keeps the in-memory envelope representation alongside the protocol buffers
// representation to avoid excess marshaling/unmarshaling.
type elem struct {
	env     *envelope.Envelope
	message *queuestore.Message
}

func (e *elem) Less(v deque.Elem) bool {
	return e.message.NextAttemptAt.Before(
		v.(*elem).message.NextAttemptAt,
	)
}
