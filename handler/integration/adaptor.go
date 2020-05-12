package integration

import (
	"context"
	"time"

	"github.com/dogmatiq/configkit"
	"github.com/dogmatiq/dodeca/logging"
	"github.com/dogmatiq/dogma"
	"github.com/dogmatiq/infix/draftspecs/envelopespec"
	"github.com/dogmatiq/infix/handler"
	"github.com/dogmatiq/infix/internal/mlog"
	"github.com/dogmatiq/infix/parcel"
	"github.com/dogmatiq/linger"
)

// Adaptor exposes a dogma.IntegrationMessageHandler as a handler.Handler.
type Adaptor struct {
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

	// Logger is the target for log messages produced within the handler.
	// If it is nil, logging.DefaultLogger is used.
	Logger logging.Logger
}

// Handle handles the message in p.
func (a *Adaptor) Handle(
	ctx context.Context,
	w *handler.UnitOfWork,
	p *parcel.Parcel,
) (err error) {
	defer mlog.LogHandlerResult(
		a.Logger,
		p.Envelope,
		a.Identity,
		configkit.IntegrationHandlerType,
		&err,
		"",
	)

	ctx, cancel := linger.ContextWithTimeout(
		ctx,
		a.Handler.TimeoutHint(p.Message),
		a.DefaultTimeout,
	)
	defer cancel()

	return a.Handler.HandleCommand(
		ctx,
		&scope{
			identity: a.Identity,
			packer:   a.Packer,
			logger:   a.Logger,
			work:     w,
			cause:    p,
		},
		p.Message,
	)
}
