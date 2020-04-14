package integration

import (
	"context"
	"time"

	"github.com/dogmatiq/dogma"
	"github.com/dogmatiq/infix/draftspecs/envelopespec"
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
	Identity *envelopespec.Identity

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
	m, err := sc.Session.Message()
	if err != nil {
		return err
	}

	ds := &scope{
		envelope: sc.Session.Envelope(),
		message:  m,
		handler:  s.Identity,
		packer:   s.Packer,
		logger:   sc.Logger,
	}

	hctx, cancel := linger.ContextWithTimeout(
		ctx,
		s.Handler.TimeoutHint(m),
		s.DefaultTimeout,
	)
	defer cancel()

	if err := s.Handler.HandleCommand(hctx, ds, m); err != nil {
		return err
	}

	for _, env := range ds.events {
		// TODO: remove this
		xxx, err := envelope.Unmarshal(s.Packer.Marshaler, env)
		if err != nil {
			panic(err)
		}

		if _, err := sc.RecordEvent(ctx, xxx); err != nil {
			return err
		}
	}

	return nil
}
