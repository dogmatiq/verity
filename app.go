package infix

import (
	"context"
	"errors"
	"fmt"

	"github.com/dogmatiq/configkit"
	"github.com/dogmatiq/configkit/message"
	"github.com/dogmatiq/dodeca/logging"
	"github.com/dogmatiq/dogma"
	"github.com/dogmatiq/infix/envelope"
	"github.com/dogmatiq/infix/eventstream"
	"github.com/dogmatiq/infix/internal/x/loggingx"
	"github.com/dogmatiq/infix/persistence"
	"github.com/dogmatiq/infix/pipeline"
	"github.com/dogmatiq/infix/queue"
)

type app struct {
	Config   configkit.RichApplication
	Stream   *eventstream.PersistedStream
	Queue    *queue.Queue
	Pipeline pipeline.Pipeline
	Logger   logging.Logger
}

func (e *Engine) initApp(
	ctx context.Context,
	cfg configkit.RichApplication,
) error {
	logging.Log(
		e.logger,
		"hosting @%s application, identity key is %s",
		cfg.Identity().Name,
		cfg.Identity().Key,
	)

	id := cfg.Identity()

	ds, err := e.dataStores.Get(ctx, id.Key)
	if err != nil {
		return fmt.Errorf(
			"unable to open data-store for %s: %w",
			cfg.Identity(),
			err,
		)
	}

	q := e.newQueue(cfg, ds)
	s := e.newEventStream(cfg, ds)
	x := e.newCommandExecutor(cfg, q)
	p := e.newPipeline(q)
	l := loggingx.WithPrefix(
		e.opts.Logger,
		"@%s  ",
		cfg.Identity().Name,
	)

	a := &app{
		Config:   cfg,
		Stream:   s,
		Queue:    q,
		Pipeline: p,
		Logger:   l,
	}

	if e.apps == nil {
		e.apps = map[string]*app{}
		e.executors = map[message.Type]dogma.CommandExecutor{}
	}

	e.apps[id.Key] = a

	for mt := range x.Packer.Roles {
		e.executors[mt] = x
	}

	return nil
}

// newQueue returns a new queue for a specific app.
func (e *Engine) newQueue(
	cfg configkit.RichApplication,
	ds persistence.DataStore,
) *queue.Queue {
	return &queue.Queue{
		DataStore: ds,
		Marshaler: e.opts.Marshaler,
		// TODO: https://github.com/dogmatiq/infix/issues/102
		// Make buffer size configurable.
		BufferSize: 0,
		Logger: loggingx.WithPrefix(
			e.logger,
			"[queue@%s] ",
			cfg.Identity().Name,
		),
	}
}

// newEventStream returns a new event stream for a specific app.
func (e *Engine) newEventStream(
	cfg configkit.RichApplication,
	ds persistence.DataStore,
) *eventstream.PersistedStream {
	return &eventstream.PersistedStream{
		App:        cfg.Identity(),
		Repository: ds.EventStoreRepository(),
		Marshaler:  e.opts.Marshaler,
		// TODO: https://github.com/dogmatiq/infix/issues/76
		// Make pre-fetch buffer size configurable.
		PreFetch: 10,
		Types: cfg.
			MessageTypes().
			Produced.
			FilterByRole(message.EventRole),
	}
}

// newCommandExecutor returns a dogma.CommandExecutor for a specific app.
func (e *Engine) newCommandExecutor(
	cfg configkit.RichApplication,
	q *queue.Queue,
) *queue.CommandExecutor {
	return &queue.CommandExecutor{
		Queue: q,
		Packer: &envelope.Packer{
			Application: cfg.Identity(),
			Roles: cfg.
				MessageTypes().
				Consumed.
				FilterByRole(message.CommandRole),
		},
	}
}

// newPipeline returns a new pipeline for a specific app.
func (e *Engine) newPipeline(
	q *queue.Queue,
) pipeline.Pipeline {
	return pipeline.Pipeline{
		pipeline.LimitConcurrency(e.semaphore),
		pipeline.WhenMessageEnqueued(
			func(ctx context.Context, messages []pipeline.EnqueuedMessage) error {
				for _, m := range messages {
					if err := q.Track(ctx, m.Memory, m.Persisted); err != nil {
						return err
					}
				}

				return nil
			},
		),
		pipeline.Acknowledge(e.opts.MessageBackoff),
		pipeline.Terminate(
			func(ctx context.Context, sc *pipeline.Scope) error {
				// TODO: we need real handlers!
				return errors.New("the truth is, there is no handler")
			},
		),
	}
}
