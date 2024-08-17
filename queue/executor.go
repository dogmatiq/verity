package queue

import (
	"context"

	"github.com/dogmatiq/dogma"
	"github.com/dogmatiq/verity/parcel"
	"github.com/dogmatiq/verity/persistence"
)

// CommandExecutor is an implementation of dogma.CommandExecutor that adds
// commands to the message queue.
type CommandExecutor struct {
	Queue     *Queue
	Persister persistence.Persister
	Packer    *parcel.Packer
}

// ExecuteCommand enqueues a command for execution.
func (x *CommandExecutor) ExecuteCommand(
	ctx context.Context,
	m dogma.Command,
	_ ...dogma.ExecuteCommandOption,
) error {
	p := x.Packer.PackCommand(m)

	qm := persistence.QueueMessage{
		NextAttemptAt: p.CreatedAt,
		Envelope:      p.Envelope,
	}

	if _, err := x.Persister.Persist(
		ctx,
		persistence.Batch{
			persistence.SaveQueueMessage{
				Message: qm,
			},
		},
	); err != nil {
		return err
	}

	qm.Revision++

	x.Queue.Add(
		[]Message{
			{qm, p},
		},
	)

	return nil
}
