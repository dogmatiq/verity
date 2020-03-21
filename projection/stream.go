package projection

import (
	"context"
	"errors"
	"time"

	"github.com/dogmatiq/configkit"
	"github.com/dogmatiq/dodeca/logging"
	"github.com/dogmatiq/dogma"
	"github.com/dogmatiq/infix/eventstream"
	"github.com/dogmatiq/infix/projection/resource"
	"github.com/dogmatiq/linger"
)

// DefaultTimeout is the default timeout to use when applying an event.
const DefaultTimeout = 3 * time.Second

// StreamAdaptor presents a dogma.ProjectionMessageHandler as an
// eventstream.Handler.
type StreamAdaptor struct {
	// Handler is the projection message handler that handles the events.
	Handler dogma.ProjectionMessageHandler

	// DefaultTimeout is the maximum time to allow for handling a single event
	// if the handler does not provide a timeout hint. If it is nil,
	// DefaultTimeout is used.
	DefaultTimeout time.Duration

	// Logger is the target for log messages produced within the handler.
	// If it is nil, logging.DefaultLogger is used.
	Logger logging.Logger
}

// NextOffset returns the next offset to be consumed from the event stream.
//
// id is the identity of the source application.
func (a *StreamAdaptor) NextOffset(
	ctx context.Context,
	id configkit.Identity,
) (uint64, error) {
	res := resource.FromApplicationKey(id.Key)

	buf, err := a.Handler.ResourceVersion(ctx, res)
	if err != nil {
		return 0, err
	}

	return resource.UnmarshalOffset(buf)
}

// HandleEvent handles a message consumed from the event stream.
//
// o must be the offset that would be returned by NextOffset(). On success,
// the next call to NextOffset() will return e.Offset + 1.
func (a *StreamAdaptor) HandleEvent(
	ctx context.Context,
	o uint64,
	ev *eventstream.Event,
) error {
	ctx, cancel := linger.ContextWithTimeout(
		ctx,
		a.Handler.TimeoutHint(ev.Envelope.Message),
		a.DefaultTimeout,
		DefaultTimeout,
	)
	defer cancel()

	ok, err := a.Handler.HandleEvent(
		ctx,
		resource.FromApplicationKey(ev.Envelope.Source.Application.Key),
		resource.MarshalOffset(o),
		resource.MarshalOffset(ev.Offset+1),
		scope{
			recordedAt: ev.Envelope.CreatedAt,
			logger:     a.Logger,
		},
		ev.Envelope.Message,
	)
	if err != nil {
		return err
	}

	if !ok {
		return errors.New("optimistic concurrency conflict")
	}

	return nil
}
