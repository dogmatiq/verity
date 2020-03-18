package memory

import (
	"container/heap"
	"context"
	"errors"
	"sync"
	"time"

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

// Get returns a transaction for a message that is ready to be handled.
func (q *Queue) Get(ctx context.Context) (persistence.QueueTransaction, error) {
	q.init()

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-q.done:
		return nil, errors.New("queue is closed")
	case m := <-q.out:
		return &transaction{
			Message: m,
		}, nil
	}
}

// Enqueue adds messages to the queue.
func (q *Queue) Enqueue(envelopes ...*envelope.Envelope) {
	q.init()

	for _, env := range envelopes {
		m := &queuedMessage{
			NextAttemptAt: env.ScheduledFor,
			Envelope:      env,
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
// Any blocked or futured calls to Get() return an error. Any future calls to
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
			delay := time.Until(next.NextAttemptAt)

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
	NextAttemptAt time.Time
	FailureCount  uint64
	Envelope      *envelope.Envelope
	Index         int
}

// messageQueue is a priority queue of messages, ordered by the "next attempt"
// time. It implements heap.Interface.
type messageQueue []*queuedMessage

func (q messageQueue) Len() int {
	return len(q)
}

func (q messageQueue) Less(i, j int) bool {
	return q[i].NextAttemptAt.Before(
		q[j].NextAttemptAt,
	)
}

func (q messageQueue) Swap(i, j int) {
	q[i], q[j] = q[j], q[i]
	q[i].Index = i
	q[j].Index = j
}

func (q *messageQueue) Push(x interface{}) {
	n := len(*q)
	m := x.(*queuedMessage)
	m.Index = n
	*q = append(*q, m)
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
	Message *queuedMessage
}

// Envelope returns the envelope containing the message to be handled
func (t *transaction) Envelope() *envelope.Envelope {
	return t.Message.Envelope
}

// Apply applies the changes from the transaction.
func (t *transaction) Apply(ctx context.Context) error {
	return errors.New("not implemented")
}

// Abort cancels the transaction, returning the message to the queue.
//
// err is the error that caused rollback, if known. A nil value does not
// indicate a success.
//
// next is the time at which the next attempt to handle the message should
// be made.
func (t *transaction) Abort(ctx context.Context, err error, next time.Time) error {
	return errors.New("not implemented")
}

// Close closes the transaction.
func (t *transaction) Close() error {
	return errors.New("not implemented")
}
