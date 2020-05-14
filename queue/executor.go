package queue

import (
	"context"

	"github.com/dogmatiq/dogma"
	"github.com/dogmatiq/infix/parcel"
	"github.com/dogmatiq/infix/persistence"
	"github.com/dogmatiq/infix/persistence/subsystem/queuestore"
)

// CommandExecutor is an implementation of dogma.CommandExecutor that adds
// commands to the message queue.
type CommandExecutor struct {
	Queue     *Queue
	Persister persistence.Persister
	Packer    *parcel.Packer
}

// ExecuteCommand enqueues a command for execution.
func (x *CommandExecutor) ExecuteCommand(ctx context.Context, m dogma.Message) error {
	p := x.Packer.PackCommand(m)

	i := queuestore.Item{
		NextAttemptAt: p.CreatedAt,
		Envelope:      p.Envelope,
	}

	if _, err := x.Persister.Persist(
		ctx,
		persistence.Batch{
			persistence.SaveQueueItem{
				Item: i,
			},
		},
	); err != nil {
		return err
	}

	i.Revision++

	x.Queue.Track(Message{p, &i})

	return nil
}
