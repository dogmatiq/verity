package integration

import (
	"context"
	"time"

	"github.com/dogmatiq/configkit"
	"github.com/dogmatiq/dogma"
	"github.com/dogmatiq/infix/envelope"
	"github.com/dogmatiq/infix/pipeline"
	"github.com/dogmatiq/linger"
)

// Sink is a pipeline sink that coordinates the handling of messages by a
// dogma.IntegrationMessageHandler.
//
// The Accept() method conforms to the pipeline.Sink() signature.
type Sink struct {
	Identity       configkit.Identity
	Handler        dogma.IntegrationMessageHandler
	Packer         *envelope.Packer
	DefaultTimeout time.Duration
}

// Accept handles a message using s.Handler.
func (s *Sink) Accept(ctx context.Context, sc *pipeline.Scope) error {
	env, err := sc.Session.Envelope(ctx)
	if err != nil {
		return err
	}

	ds := &scope{
		handler: s.Identity,
		cause:   env,
		packer:  s.Packer,
		logger:  sc.Logger,
	}

	hctx, cancel := linger.ContextWithTimeout(
		ctx,
		s.Handler.TimeoutHint(env.Message),
		s.DefaultTimeout,
	)
	defer cancel()

	if err := s.Handler.HandleCommand(hctx, ds, env.Message); err != nil {
		return err
	}

	for _, env := range ds.events {
		if _, err := sc.RecordEvent(ctx, env); err != nil {
			return err
		}
	}

	return nil
}
