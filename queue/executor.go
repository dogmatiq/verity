package queue

import (
	"context"
	"time"

	"github.com/dogmatiq/dogma"
	"github.com/dogmatiq/infix/envelope"
	"github.com/dogmatiq/infix/persistence"
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
	qm := x.Queue.NewMessage(env, time.Now())

	if err := persistence.WithTransaction(
		ctx,
		x.Queue.DataStore,
		func(tx persistence.ManagedTransaction) error {
			return tx.SaveMessageToQueue(ctx, qm)
		},
	); err != nil {
		return err
	}

	qm.Revision++

	return x.Queue.Track(ctx, env, qm)
}
