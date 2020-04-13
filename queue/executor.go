package queue

import (
	"context"
	"time"

	"github.com/dogmatiq/dogma"
	"github.com/dogmatiq/infix/envelope"
	"github.com/dogmatiq/infix/persistence"
	"github.com/dogmatiq/infix/persistence/subsystem/queuestore"
)

// CommandExecutor is an implementation of dogma.CommandExecutor that adds
// commands to the message queue.
type CommandExecutor struct {
	Queue  *Queue
	Packer *envelope.Packer
}

// ExecuteCommand enqueues a command for execution.
func (x *CommandExecutor) ExecuteCommand(ctx context.Context, m dogma.Message) error {
	env := x.Packer.PackCommand(m)
	p := x.Queue.NewParcel(env, time.Now())

	if err := persistence.WithTransaction(
		ctx,
		x.Queue.DataStore,
		func(tx persistence.ManagedTransaction) error {
			return tx.SaveMessageToQueue(ctx, p)
		},
	); err != nil {
		return err
	}

	p.Revision++

	return x.Queue.Track(
		ctx,
		queuestore.Pair{
			Parcel:   p,
			Original: env,
		},
	)
}
