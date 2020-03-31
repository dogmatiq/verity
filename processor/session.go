package processor

import (
	"context"

	"github.com/dogmatiq/infix/persistence"
	"github.com/dogmatiq/infix/persistence/subsystem/queue"
)

type Session struct {
	Message     *queue.Message
	Transaction persistence.Transaction
}

func (s *Session) Run(ctx context.Context) error {
	defer s.Semaphore.Release(1)
	return ctx.Err()
}
