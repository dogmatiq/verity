package queue

import (
	"context"
	"errors"
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

	m      sync.Mutex
	buffer deque.Deque
}

// Pop removes the first message from the queue.
//
// It blocks until a message is ready to be handled or ctx is canceled.
// It returns a session within which the message is to be handled.
func (q *Queue) Pop(ctx context.Context) (_ *Session, err error) {
	q.m.Lock()
	defer q.m.Unlock()
	x, ok := q.buffer.PeekFront()

	if !ok {
		<-ctx.Done()
		return nil, ctx.Err()
	}

	e := x.(*elem)

	if e.message.NextAttemptAt.After(time.Now()) {
		return nil, errors.New("not implemented")
	}

	tx, err := q.DataStore.Begin(ctx)
	if err != nil {
		return nil, err
	}

	q.buffer.PopFront()

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

	tx, err := q.DataStore.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	if err := tx.SaveMessageToQueue(ctx, e.message); err != nil {
		return err
	}

	if err := tx.Commit(ctx); err != nil {
		return err
	}

	q.m.Lock()
	defer q.m.Unlock()

	q.buffer.Push(e)

	return nil
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
