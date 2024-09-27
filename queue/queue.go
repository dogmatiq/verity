package queue

import (
	"context"
	"runtime"
	"sync"
	"time"

	"github.com/dogmatiq/enginekit/marshaler"
	"github.com/dogmatiq/kyu"
	"github.com/dogmatiq/verity/parcel"
	"github.com/dogmatiq/verity/persistence"
)

// DefaultBufferSize is the default size of the in-memory queue buffer.
var DefaultBufferSize = runtime.GOMAXPROCS(0) * 10

// A Queue is an prioritized collection of messages.
//
// It is an in-memory representation of the head of the persisted message queue.
//
// It dispatches to multiple consumers, ensuring each consumer receives a
// different message.
type Queue struct {
	// Repository is used to load messages from the queue whenever the in-memory
	// buffer is exhausted.
	Repository persistence.QueueRepository

	// Marshaler is used to unmarshal the messages loaded via the repository.
	Marshaler marshaler.Marshaler

	// BufferSize is the maximum number of messages to buffer in memory.
	// If it is non-positive, DefaultBufferSize is used.
	//
	// It should be larger than the number of concurrent consumers.
	BufferSize int

	// tracked is the set of all message IDs that are currently "managed" by
	// this Queue instance. All tracked messages are already persisted.
	//
	// There may be more messages on the persisted message queue than are loaded
	// into memory, as determined by BufferSize.
	//
	// The messages in memory are always those with the highest-priority, that
	// is, those that are scheduled to be handled the soonest.
	tracked map[string]struct{}

	// pending is a double-ended priority queue containg all messages that are
	// waiting to be handled.
	//
	// It is use to determine which message should be handled next (those at the
	// front of the queue), and which messages should be purged when the number
	// of tracked messages exceeds BufferSize (those at the back of the queue).
	//
	// All pending messages are tracked but not all tracked messages are
	// necessarily pending, instead they may have already been obtained by a
	// consumer via Pop() and be in the process of being handled.
	pending kyu.PDeque

	// timeouts is an index of tracked timeout messages, keyed by the process
	// handler and instance ID that produced them.
	//
	// This index is maintained to allow efficient removal of all timeout
	// messages for a specific instance in the RemoveTimeoutsByProcessID()
	// message.
	timeouts map[processID]map[string]*kyu.Element

	// exhaustive is a flag that indicates that all persisted messages are known
	// to be loaded into memory.
	//
	// Once an attempt to load messages is made that does not entirely fill the
	// buffer size the in-memory queue is said be exhaustive. It will not
	// attempt to load any more messages from the data-store in the future
	// unless the buffer size is exceeded by an influx of new messages.
	exhaustive bool

	once      sync.Once
	done      chan struct{}    // closed when Run() exits
	pop       chan Message     // delivers messages to consumers
	mutations chan func() bool // sends "mutation" function to Run()
}

// processID encapsulates a process handler key and instance ID as a single
// value.
//
// It is used as the key for the timeout index used to allow somewhat efficient
// implementation of Queue.RemoveTimeoutsByProcessID().
type processID struct {
	HandleKey  string
	InstanceID string
}

// Pop returns the message at the front of the queue.
//
// It blocks until a message is ready to be handled or ctx is canceled.
//
// Once the message has been handled it must either be removed from the queue
// entirely, or returned to the pending queue, by calling q.Ack() or q.Nack(),
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

// Ack stops tracking a message that was obtained via Pop() and has been handled
// successfully.
func (q *Queue) Ack(m Message) {
	q.mutate(func() bool {
		delete(q.tracked, m.ID())

		if !m.Parcel.ScheduledFor.IsZero() {
			q.unindexTimeout(m)
		}

		return !q.exhaustive && len(q.tracked) == 0
	})
}

// Nack re-queues a message that was obtained via Pop() but was not handled
// successfully.
//
// The message is placed in the queue according to the current value of
// m.NextAttemptAt under the assumption it has been updated after the failure.
func (q *Queue) Nack(m Message) {
	q.mutate(func() bool {
		e := q.pending.Push(m)

		if !m.Parcel.ScheduledFor.IsZero() {
			q.indexTimeout(m, e)
		}

		return q.pending.IsFront(e)
	})
}

// RemoveTimeoutsByProcessID removes any timeout messages that originated from a
// specific process instance ID.
//
// hk is the process's handler key, id is the instance ID.
func (q *Queue) RemoveTimeoutsByProcessID(hk, id string) {
	q.mutate(func() bool {
		pid := processID{hk, id}

		timeouts := q.timeouts[pid]
		delete(q.timeouts, pid)

		head := false
		for id, e := range timeouts {
			if q.pending.IsFront(e) {
				head = true
			}

			q.pending.Remove(e)
			delete(q.tracked, id)
		}

		return head
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

	if q.exhaustive {
		// There's no point in loading anything if we already know there's
		// nothing to load.
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

	// Start tracking the messages.
	for _, m := range messages {
		if _, ok := q.tracked[m.ID()]; ok {
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
		q.tracked[m.ID()] = struct{}{}

		if !m.Parcel.ScheduledFor.IsZero() {
			q.indexTimeout(m, e)
		}

		if q.pending.IsFront(e) {
			// This message became the new head of the queue.
			head = true
		}
	}

	q.purge()

	return head
}

// purge drops the lowest-priority messages until the number of tracked messages
// falls below the buffer size limit.
func (q *Queue) purge() {
	limit := q.BufferSize
	if limit <= 0 {
		limit = DefaultBufferSize
	}

	for len(q.tracked) > limit {
		x, _ := q.pending.PopBack()
		m := x.(Message)
		delete(q.tracked, m.ID())

		if !m.Parcel.ScheduledFor.IsZero() {
			q.unindexTimeout(m)
		}

		q.exhaustive = false
	}
}

// indexTimeout adds m to the index of timeout messages.
func (q *Queue) indexTimeout(m Message, e *kyu.Element) {
	pid := processID{
		m.Envelope.SourceHandler.Key,
		m.Envelope.SourceInstanceId,
	}

	timeouts := q.timeouts[pid]
	if timeouts == nil {
		timeouts = map[string]*kyu.Element{}
		q.timeouts[pid] = timeouts
	}

	timeouts[m.ID()] = e
}

// unindexTimeout removes m from the index of timeout messages.
func (q *Queue) unindexTimeout(m Message) {
	pid := processID{
		m.Envelope.SourceHandler.Key,
		m.Envelope.SourceInstanceId,
	}

	timeouts := q.timeouts[pid]

	delete(timeouts, m.ID())

	if len(timeouts) == 0 {
		delete(q.timeouts, pid)
	}
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
		q.timeouts = make(map[processID]map[string]*kyu.Element)

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
