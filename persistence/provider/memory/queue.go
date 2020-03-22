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

// Queue is an implementation of persistence.Queue that stores messages in
// memory.
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
func (q *Queue) Begin(ctx context.Context) (persistence.Transaction, *envelope.Envelope, error) {
	q.init()

	select {
	case <-ctx.Done():
		return nil, nil, ctx.Err()
	case <-q.done:
		return nil, nil, errors.New("queue is closed")
	case m := <-q.out:
		return &transaction{}, m.env, nil
	}
}

// Enqueue adds a message to the queue.
func (q *Queue) Enqueue(ctx context.Context, env *envelope.Envelope) error {
	q.init()

	m := &queuedMessage{
		next: env.ScheduledFor,
		env:  env,
	}

	select {
	case q.in <- m:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-q.done:
		return errors.New("queue is closed")
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
			delay := time.Until(next.next)

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
	next     time.Time
	failures uint64
	env      *envelope.Envelope
}

// messageQueue is a priority queue of messages, ordered by the "next attempt"
// time. It implements heap.Interface.
type messageQueue []*queuedMessage

func (q messageQueue) Len() int {
	return len(q)
}

func (q messageQueue) Less(i, j int) bool {
	return q[i].next.Before(
		q[j].next,
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
