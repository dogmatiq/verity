package processor

import (
	"context"
	"runtime"
	"sync"
	"time"

	"github.com/dogmatiq/infix/persistence/subsystem/queue"
)

// DefaultBufferSize is the default size of the in-memory queue buffer.
var DefaultBufferSize = runtime.GOMAXPROCS(0) * 10

// Queue provides concurrent access to an application's message queue.
type Queue struct {
	// Repository is used to load messages from the persisted queue.
	Repository queue.Repository

	// BufferSize is the maximum number of messages to buffer in memory.
	// If it is non-positive, DefaultBufferSize is used.
	BufferSize int

	once     sync.Once
	in       chan *queue.Message
	out      chan *queue.Message
	complete bool // true if all persisted messages are in memory
	pq       pqueue
}

// Pop removes a message that is ready for handling and returns it.
//
// It blocks until a message is ready for handling, or until ctx is canceled.
func (q *Queue) Pop(ctx context.Context) (*queue.Message, error) {
	q.init()

	select {
	case out := <-q.out:
		return out, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// Push adds (or returns) a message to the queue.
//
// It is assumed that the message has already been persisted.
func (q *Queue) Push(ctx context.Context, m *queue.Message) error {
	q.init()

	select {
	case q.in <- m:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Run starts the queue, which coordinates loading persisted messages and
// dispatching them to waiting calls to Pop().
func (q *Queue) Run(ctx context.Context) error {
	q.init()

	for {
		out, ok := q.pq.PeekFront()

		if ok {
			return q.dispatch(ctx, out)
		} else if !q.complete {
			return q.load(ctx)
		}

		// We've got everything in memory, so all we can do is wait until
		// something new is pushed.
		select {
		case in := <-q.in:
			q.push(in)
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// dispatch sends out to a waiting Pop() call.
func (q *Queue) dispatch(ctx context.Context, out *queue.Message) error {
	delay := time.Until(out.NextAttemptAt)

	if delay > 0 {
		ready, err := q.wait(ctx, delay)
		if !ready || err != nil {
			return err
		}
	}

	for {
		select {
		case in := <-q.in:
			if q.push(in) {
				return nil
			}
		case q.out <- out:
			q.pq.PopFront()
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// wait blocks until d elapses, or until a new message is pushed to the head of
// the queue.
//
// It returns true if the time elapses without a message being pushed.
func (q *Queue) wait(ctx context.Context, d time.Duration) (bool, error) {
	timer := time.NewTimer(d)
	defer timer.Stop()

	for {
		select {
		case <-timer.C:
			return true, nil
		case in := <-q.in:
			if q.push(in) {
				return false, nil
			}
		case <-ctx.Done():
			return false, ctx.Err()
		}
	}
}

// load fills the in-memory queue with persisted messages.
func (q *Queue) load(ctx context.Context) error {
	if q.pq.Len() != 0 {
		// CODE COVERAGE: This should never be allowed to occur, as it could
		// result in duplicate messages in the priority-queue.
		panic("can't load messages into non-empty queue")
	}

	size := q.BufferSize
	if size <= 0 {
		size = DefaultBufferSize
	}

	messages, err := q.Repository.LoadQueueMessages(ctx, size)
	if err != nil {
		return err
	}

	for _, m := range messages {
		q.pq.Push(m)
	}

	if len(messages) < size {
		// If we didn't get back as many message as we asked for, we know that
		// we've loaded everything that has been persisted.
		q.complete = true
	}

	return nil
}

// push adds a message (that has already been persisted) to the queue.
func (q *Queue) push(m *queue.Message) bool {
	front := q.pq.Push(m)

	size := q.BufferSize
	if size <= 0 {
		size = DefaultBufferSize
	}

	if q.pq.Len() > size {
		q.pq.PopBack()

		// We've had to remove a message from the buffer, so now there are
		// persisted messages that are not in memory.
		q.complete = false
	}

	return front
}

func (q *Queue) init() {
	q.once.Do(func() {
		q.in = make(chan *queue.Message)
		q.out = make(chan *queue.Message)
	})
}
