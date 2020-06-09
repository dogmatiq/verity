package queue

import (
	"context"
	"runtime"
	"sync"
	"time"

	"github.com/dogmatiq/infix/parcel"
	"github.com/dogmatiq/infix/persistence"
	"github.com/dogmatiq/kyu"
	"github.com/dogmatiq/marshalkit"
)

// DefaultBufferSize is the default size of the in-memory queue buffer.
var DefaultBufferSize = runtime.GOMAXPROCS(0) * 10

// A Queue is an prioritized collection of messages.
//
// It exposes an application's message queue to multiple consumers, ensuring
// each consumer receives a different message.
type Queue struct {
	// Repository is used to load messages from the queue whenever the in-memory
	// buffer is exhausted.
	Repository persistence.QueueRepository

	// Marshaler is used to unmarshal the messages loaded via the repository.
	Marshaler marshalkit.ValueMarshaler

	// BufferSize is the maximum number of messages to buffer in memory.
	// If it is non-positive, DefaultBufferSize is used.
	//
	// It should be larger than the number of concurrent consumers.
	BufferSize int

	// A "tracked" message is a message that is being managed by this Queue. All
	// tracked messages are already persisted in the data-store.
	//
	// The tracked messages are always those with the highest-priority, that is,
	// those that are scheduled to be handled the soonest.
	//
	// Every tracked message has either been obtained via Pop(), or it's still
	// in the "pending" queue.
	tracked    map[string]struct{} // key == message ID
	pending    kyu.PDeque          // priority queue of messages that haven't been popped
	exhaustive bool                // true if all queued messages are in memory

	once      sync.Once
	done      chan struct{}    // closed when Run() exits
	pop       chan Message     // delivers messages to consumers
	mutations chan func() bool // sends "mutation" function to Run()
}

// Pop returns the message at the front of the queue.
//
// It blocks until a message is ready to be handled or ctx is canceled.
//
// Once the message has been handled it must either be returned to the pending
// queue, or removed entirely by calling q.Requeue() or q.Remove(),
// respectively.
func (q *Queue) Pop(ctx context.Context) (Message, error) {
	q.init()

	select {
	case <-ctx.Done():
		return Message{}, ctx.Err()
	case m := <-q.pop:
		return m, nil
	}
}

// Add begins tracking messages that have already been persisted.
func (q *Queue) Add(messages []Message) {
	q.init()
	q.mutate(func() bool {
		return q.track(messages)
	})
}

// Requeue returns a popped message to the queue.
func (q *Queue) Requeue(m Message) {
	q.mutate(func() bool {
		e := q.pending.Push(m)
		return q.pending.IsFront(e)
	})
}

// Remove stops tracking a popped message.
func (q *Queue) Remove(m Message) {
	q.mutate(func() bool {
		delete(q.tracked, m.ID())
		return !q.exhaustive && len(q.tracked) == 0
	})
}

// mutate performs some modification to the queue in the context of Run().
//
// If fn() returns true, Run() starts a new call to tick(), re-evaluating the
// message at the head of the queue.
//
// It returns once the mutation has been received by Run()'s goroutine, but does
// NOT block until fn() has returned.
func (q *Queue) mutate(fn func() bool) {
	select {
	case q.mutations <- fn:
		return // keep to see coverage
	case <-q.done:
		return // keep to see coverage
	}
}

// Run starts the queue.
//
// It coordinates the tracking of messages that are loaded from a queue
// repository or manually added to the queue by Add().
func (q *Queue) Run(ctx context.Context) error {
	q.init()
	defer close(q.done)

	for {
		if err := q.tick(ctx); err != nil {
			return err
		}
	}
}

// peek returns the message at the front of the queue, loading messages from the
// store if none are currently tracked.
func (q *Queue) peek(ctx context.Context) (Message, bool, error) {
	if e, ok := q.pending.Peek(); ok {
		return e.Value.(Message), true, nil
	}

	if err := q.load(ctx); err != nil {
		return Message{}, false, err
	}

	if e, ok := q.pending.Peek(); ok {
		return e.Value.(Message), true, nil
	}

	return Message{}, false, nil
}

// tick performs one "step" of Run().
func (q *Queue) tick(ctx context.Context) error {
	var (
		// Setup channel variables that we only populate if we want to use
		// them. Otherwise we leave them nil, causing them to block forever,
		// having no effect on the select below.
		pop  chan<- Message
		wait <-chan time.Time
	)

	m, ok, err := q.peek(ctx)
	if err != nil {
		return err
	}

	if ok {
		// There is a message at the head of the queue.
		d := time.Until(m.NextAttemptAt)

		if d <= 0 {
			// The message is ready to be handled now.
			pop = q.pop
		} else {
			// The message is not ready to be handled yet.
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
			// The message has become ready to handle.
			pop = q.pop
			wait = nil

		case pop <- m:
			// The message was delivered to a consumer.
			q.pending.Pop()
			return nil

		case fn := <-q.mutations:
			if fn() {
				// The action signalled that the head of the queue should be
				// re-evaluated, so we return to start a new loop.
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
		return nil
	}

	limit := q.BufferSize
	if limit <= 0 {
		limit = DefaultBufferSize
	}

	// Load messages up to our configured limit.
	persisted, err := q.Repository.LoadQueueMessages(ctx, limit)
	if err != nil {
		return err
	}

	if len(persisted) < limit {
		// We didn't get as many messages as we requested, so we know we have
		// everything in memory.
		q.exhaustive = true
	}

	messages := make([]Message, len(persisted))
	for i, m := range persisted {
		p, err := parcel.FromEnvelope(q.Marshaler, m.Envelope)
		if err != nil {
			return err
		}

		messages[i] = Message{m, p}
	}

	q.track(messages)

	return nil
}

// track starts tracking the given messages.
//
// It returns true if any of the given messages becomes the head of the queue.
func (q *Queue) track(messages []Message) bool {
	head := false
	size := len(q.tracked)

	// Start tracking the messages.
	for _, m := range messages {
		q.tracked[m.ID()] = struct{}{}

		if size == len(q.tracked) {
			// The message was already in the tracked list, which could occur if
			// it was persisted around the same time that we loaded from the
			// store, such that the order of operations was:
			//
			// - message persisted
			// - messages loaded
			// - Add() called
			continue
		}

		e := q.pending.Push(m)

		if q.pending.IsFront(e) {
			// This message became the new head of the queue.
			head = true
		}

		size++
	}

	limit := q.BufferSize
	if limit <= 0 {
		limit = DefaultBufferSize
	}

	if size > limit {
		// The number of tracked messages exceeds the limit. We need to start
		// dropping tracked messages until the count falls below the limit
		// again.
		q.exhaustive = false

		for size > limit {
			m, _ := q.pending.PopBack()
			delete(q.tracked, m.(Message).ID())
			size--
		}
	}

	return head
}

// init initializes the queue's internal state.
func (q *Queue) init() {
	q.once.Do(func() {
		limit := q.BufferSize
		if limit <= 0 {
			limit = DefaultBufferSize
		}

		// Allocate capcity for our limit +1 message used to detect overflow.
		q.tracked = make(map[string]struct{}, limit+1)

		q.pending.Less = func(a, b interface{}) bool {
			return a.(Message).NextAttemptAt.Before(
				b.(Message).NextAttemptAt,
			)
		}

		q.done = make(chan struct{})
		q.pop = make(chan Message)
		q.mutations = make(chan func() bool)
	})
}
