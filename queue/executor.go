package queue

import (
	"context"
	"time"

	"github.com/dogmatiq/dogma"
	"github.com/dogmatiq/infix/parcel"
	"github.com/dogmatiq/infix/persistence"
	"github.com/dogmatiq/infix/persistence/subsystem/queuestore"
)

// CommandExecutor is an implementation of dogma.CommandExecutor that adds
// commands to the message queue.
type CommandExecutor struct {
	Queue  *Queue
	Packer *parcel.Packer
}

// ExecuteCommand enqueues a command for execution.
func (x *CommandExecutor) ExecuteCommand(ctx context.Context, m dogma.Message) error {
	p := x.Packer.PackCommand(m)
	i := &queuestore.Item{
		NextAttemptAt: time.Now(),
		Envelope:      p.Envelope,
	}

	if err := persistence.WithTransaction(
		ctx,
		x.Queue.DataStore,
		func(tx persistence.ManagedTransaction) error {
			return tx.SaveMessageToQueue(ctx, i)
		},
	); err != nil {
		return err
	}

	i.Revision++

	return x.Queue.Track(ctx, p, i)
}
