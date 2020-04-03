package queue

import (
	"context"
	"runtime"
	"time"

	"github.com/dogmatiq/infix/envelope"
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
}

// Push adds a message to the queue.
func (q *Queue) Push(ctx context.Context, env *envelope.Envelope) error {
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
