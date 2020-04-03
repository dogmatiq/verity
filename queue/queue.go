package queue

import (
	"context"
	"time"

	"github.com/dogmatiq/infix/envelope"
	"github.com/dogmatiq/infix/persistence"
	"github.com/dogmatiq/infix/persistence/subsystem/queuestore"
	"github.com/dogmatiq/marshalkit"
)

// A Queue is an prioritized collection of messages.
//
// It implements the dogma.CommandExecutor interface.
type Queue struct {
	DataStore persistence.DataStore
	Marshaler marshalkit.ValueMarshaler
}

// Enqueue adds a message to the queue.
func (q *Queue) Enqueue(ctx context.Context, env *envelope.Envelope) error {
	it := &item{
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

	if err := tx.SaveMessageToQueue(ctx, it.message); err != nil {
		return err
	}

	return tx.Commit(ctx)
}

// item is a container for a queued message that is buffered in memory.
//
// It keeps the in-memory envelope representation alongside the protocol buffers
// representation to avoid excess marshaling/unmarshaling.
type item struct {
	env     *envelope.Envelope
	message *queuestore.Message
}
