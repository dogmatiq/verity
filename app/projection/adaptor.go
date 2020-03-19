package projection

import (
	"context"
	"errors"
	"time"

	"github.com/dogmatiq/dodeca/logging"
	"github.com/dogmatiq/dogma"
	"github.com/dogmatiq/infix/app/projection/resource"
	"github.com/dogmatiq/infix/persistence"
	"github.com/dogmatiq/linger"
)

// DefaultTimeout is the default timeout to use when applying an event.
const DefaultTimeout = 3 * time.Second

// An Adaptor persents a dogma.ProjectionMessageHandler as a
// persistence.StreamHandler.
type Adaptor struct {
	// Handler is the projection message handler the events are applied to.
	Handler dogma.ProjectionMessageHandler

	// DefaultTimeout is the maximum time to allow for handling a single event
	// if the handler does not provide a timeout hint. If it is nil,
	// DefaultTimeout is used.
	DefaultTimeout time.Duration

	// Logger is the target for log messages the handler.
	// If it is nil, logging.DefaultLogger is used.
	Logger logging.Logger
}

// NextOffset returns the next offset to be consumed from the event stream.
//
// k is the identity key of the source application.
func (a *Adaptor) NextOffset(ctx context.Context, k string) (uint64, error) {
	res := resource.FromApplicationKey(k)

	buf, err := a.Handler.ResourceVersion(ctx, res)
	if err != nil {
		return 0, err
	}

	return resource.UnmarshalOffset(buf)
}

// HandleEvent handles a message consumed from the event stream.
//
// o is the offset value returned by NextOffset().
func (a *Adaptor) HandleEvent(
	ctx context.Context,
	o uint64,
	m *persistence.StreamMessage,
) error {
	ctx, cancel := linger.ContextWithTimeout(
		ctx,
		a.Handler.TimeoutHint(m.Envelope.Message),
		a.DefaultTimeout,
		DefaultTimeout,
	)
	defer cancel()

	ok, err := a.Handler.HandleEvent(
		ctx,
		resource.FromApplicationKey(m.Envelope.Source.Application.Key),
		resource.MarshalOffset(o),
		resource.MarshalOffset(m.Offset+1),
		scope{
			recordedAt: m.Envelope.CreatedAt,
			logger:     a.Logger,
		},
		m.Envelope.Message,
	)
	if err != nil {
		return err
	}

	if !ok {
		return errors.New("optimistic concurrency conflict")
	}

	return nil
}
