package infix

import (
	"context"
	"fmt"

	"github.com/dogmatiq/configkit"
	"github.com/dogmatiq/configkit/message"
	"github.com/dogmatiq/dodeca/logging"
	"github.com/dogmatiq/infix/envelope"
	"github.com/dogmatiq/infix/eventstream"
	"github.com/dogmatiq/infix/queue"
)

type app struct {
	Config         configkit.RichApplication
	Stream         *eventstream.PersistedStream
	Queue          *queue.Queue
	InternalPacker *envelope.Packer
	ExternalPacker *envelope.Packer
}

func (e *Engine) initApp(
	ctx context.Context,
	cfg configkit.RichApplication,
) error {
	logging.Log(
		e.logger,
		"initializing @%s application, identity key is %s",
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

	commands := cfg.MessageTypes().Consumed.FilterByRole(message.CommandRole)

	a := &app{
		Config: cfg,
		Stream: &eventstream.PersistedStream{
			App:        cfg.Identity(),
			Types:      cfg.MessageTypes().Produced.FilterByRole(message.EventRole),
			Repository: ds.EventStoreRepository(),
			Marshaler:  e.opts.Marshaler,
			// TODO: https://github.com/dogmatiq/infix/issues/76
			// Make pre-fetch buffer size configurable.
			PreFetch: 10,
		},
		Queue: &queue.Queue{
			DataStore: ds,
			Marshaler: e.opts.Marshaler,
			// TODO: https://github.com/dogmatiq/infix/issues/102
			// Make buffer size configurable.
			BufferSize: 0,
			Logger:     e.logger,
		},
		InternalPacker: &envelope.Packer{
			Application: id,
			Roles:       cfg.MessageTypes().Produced,
		},
		ExternalPacker: &envelope.Packer{
			Application: id,
			Roles:       commands,
		},
	}

	if e.appsByKey == nil {
		e.appsByKey = map[string]*app{}
		e.appsByCommand = map[message.Type]*app{}
	}

	e.appsByKey[id.Key] = a

	for mt := range commands {
		e.appsByCommand[mt] = a
	}

	return nil
}
