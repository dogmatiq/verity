package integration

import (
	"context"
	"time"

	"github.com/dogmatiq/dogma"
	"github.com/dogmatiq/infix/draftspecs/envelopespec"
	"github.com/dogmatiq/infix/parcel"
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

	// Packer is used to create new parcels for events recorded by the
	// handler.
	Packer *parcel.Packer

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
		cause: &parcel.Parcel{
			Envelope: sc.Session.Envelope(),
			Message:  m,
		},
		packer:  s.Packer,
		handler: s.Identity,
		logger:  sc.Logger,
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
		if _, err := sc.RecordEvent(ctx, env); err != nil {
			return err
		}
	}

	return nil
}
