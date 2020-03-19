package memory

import (
	"container/heap"
	"context"
	"errors"
	"sync"
	"time"

	"github.com/dogmatiq/dogma"
	"github.com/dogmatiq/infix/envelope"
	"github.com/dogmatiq/infix/persistence"
)

// Queue is an implementation of persistence.Queue that stores messages
// in-memory.
type Queue struct {
	m      sync.Mutex
	closed bool
	done   chan struct{}
	in     chan *queuedMessage
	out    chan *queuedMessage
}

// Begin starts a transaction for a message on the application's message
// queue that is ready to be handled.
//
// If no messages are ready to be handled, it blocks until one becomes
// ready, ctx is canceled, or an error occurs.
func (q *Queue) Begin(ctx context.Context) (persistence.Transaction, error) {
	q.init()

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-q.done:
		return nil, errors.New("queue is closed")
	case m := <-q.out:
		return &transaction{
			message: m,
		}, nil
	}
}

// Enqueue adds messages to the queue.
func (q *Queue) Enqueue(envelopes ...*envelope.Envelope) {
	q.init()

	for _, env := range envelopes {
		m := &queuedMessage{
			nextAttemptAt: env.ScheduledFor,
			envelope:      env,
		}

		select {
		case q.in <- m:
			continue // keep to see coverage
		case <-q.done:
			panic("queue is closed")
		}
	}
}

// Close closes the queue.
//
// Any blocked or future calls to Begin() return an error. Any future calls to
// Enqueue() will panic.
func (q *Queue) Close() error {
	q.m.Lock()
	defer q.m.Unlock()

	if q.closed {
		return errors.New("queue is already closed")
	}

	if q.done == nil {
		q.done = make(chan struct{})
	}

	q.closed = true
	close(q.done)

	return nil
}

// init initializes the queue's channels.
func (q *Queue) init() {
	q.m.Lock()
	defer q.m.Unlock()

	if q.closed {
		return
	}

	if q.done == nil {
		q.done = make(chan struct{})
	}

	if q.in == nil {
		q.in = make(chan *queuedMessage)
		q.out = make(chan *queuedMessage)

		go q.run()
	}
}

// run dispatches messages to waiting calls to Next() as they become ready for
// handling.
//
// It returns when Close() is called.
func (q *Queue) run() {
	var (
		messages messageQueue
		timer    *time.Timer
		wait     <-chan time.Time
		send     chan<- *queuedMessage
		next     *queuedMessage
	)

	defer func() {
		if timer != nil {
			timer.Stop()
		}
	}()

	for {
		if timer != nil {
			timer.Stop()
			timer = nil
		}

		next = nil
		wait = nil
		send = nil

		if len(messages) != 0 {
			next = messages[0]
			delay := time.Until(next.nextAttemptAt)

			if delay > 0 {
				timer = time.NewTimer(delay)
				wait = timer.C
			} else {
				send = q.out
			}
		}

		select {
		case m := <-q.in:
			heap.Push(&messages, m)
		case send <- next:
			heap.Pop(&messages)
		case <-wait:
			continue // keep to see coverage
		case <-q.done:
			return
		}
	}
}

// queuedMessage is a container for a message on a priority queue.
type queuedMessage struct {
	nextAttemptAt time.Time
	failureCount  uint64
	envelope      *envelope.Envelope
}

// messageQueue is a priority queue of messages, ordered by the "next attempt"
// time. It implements heap.Interface.
type messageQueue []*queuedMessage

func (q messageQueue) Len() int {
	return len(q)
}

func (q messageQueue) Less(i, j int) bool {
	return q[i].nextAttemptAt.Before(
		q[j].nextAttemptAt,
	)
}

func (q messageQueue) Swap(i, j int) {
	q[i], q[j] = q[j], q[i]
}

func (q *messageQueue) Push(x interface{}) {
	*q = append(*q, x.(*queuedMessage))
}

func (q *messageQueue) Pop() interface{} {
	n := len(*q)
	m := (*q)[n-1]
	(*q)[n-1] = nil // avoid memory leak
	*q = (*q)[:n-1]

	return m
}

// transaction is an implementation of persistence.QueueTransaction for
// in-memory persistence.
type transaction struct {
	message *queuedMessage
}

// Envelope returns the envelope containing the message to be handled
func (t *transaction) Envelope(context.Context) (*envelope.Envelope, error) {
	return t.message.envelope, nil
}

// PersistAggregate updates (or creates) an aggregate instance.
func (t *transaction) PersistAggregate(
	ctx context.Context,
	ref persistence.InstanceRef,
	r dogma.AggregateRoot,
) error {
	return errors.New("not implemented")
}

// PersistProcess updates (or creates) a process instance.
func (t *transaction) PersistProcess(
	ctx context.Context,
	ref persistence.InstanceRef,
	r dogma.ProcessRoot,
) error {
	return errors.New("not implemented")
}

// Delete deletes an aggregate or process instance.
func (t *transaction) Delete(ctx context.Context, ref persistence.InstanceRef) error {
	return errors.New("not implemented")
}

// PersistMessage adds a message to the application's message queue and/or
// event stream as appropriate.
func (t *transaction) PersistMessage(ctx context.Context, env *envelope.Envelope) error {
	return errors.New("not implemented")
}

// Apply applies the changes from the transaction.
func (t *transaction) Apply(ctx context.Context) error {
	return errors.New("not implemented")
}

// Abort cancels the transaction, returning the message to the queue.
//
// next indicates when the message should be retried.
func (t *transaction) Abort(ctx context.Context, next time.Time) error {
	return errors.New("not implemented")
}

// Close closes the transaction.
func (t *transaction) Close() error {
	return errors.New("not implemented")
}
