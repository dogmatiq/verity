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
	// Identity is the handler's identity.
	Identity configkit.Identity

	// Handler is the integration message handler that implements the
	// application-specific message handling logic.
	Handler dogma.IntegrationMessageHandler

	// Packer is used to create new envelopes for events recorded by the
	// handler.
	Packer *envelope.Packer

	// DefaultTimeout is the timeout to apply when handling the message if the
	// handler does not provide a timeout hint.
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