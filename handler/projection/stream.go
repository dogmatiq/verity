package projection

import (
	"context"
	"errors"
	"time"

	"github.com/dogmatiq/configkit"
	"github.com/dogmatiq/dodeca/logging"
	"github.com/dogmatiq/dogma"
	"github.com/dogmatiq/enginekit/protobuf/identitypb"
	"github.com/dogmatiq/linger"
	"github.com/dogmatiq/verity/eventstream"
	"github.com/dogmatiq/verity/internal/mlog"
)

// DefaultTimeout is the default timeout to use when applying an event.
const DefaultTimeout = 3 * time.Second

// StreamAdaptor presents a dogma.ProjectionMessageHandler as an
// eventstream.Handler.
type StreamAdaptor struct {
	// Identity is the handler's identity.
	Identity *identitypb.Identity

	// Handler is the projection message handler that handles the events.
	Handler dogma.ProjectionMessageHandler

	// Timeout is the maximum time to allow for handling a single event. If it
	// is non-positive, DefaultTimeout is used.
	Timeout time.Duration

	// Logger is the target for log messages produced within the handler.
	// If it is nil, logging.DefaultLogger is used.
	Logger logging.Logger
}

// NextOffset returns the next offset to be consumed from the event stream.
//
// id is the identity of the source application.
func (a *StreamAdaptor) NextOffset(
	ctx context.Context,
	id *identitypb.Identity,
) (uint64, error) {
	return a.Handler.CheckpointOffset(ctx, id.Key.AsString())
}

// HandleEvent handles a message consumed from the event stream.
//
// o must be the offset that would be returned by NextOffset(). On success,
// the next call to NextOffset() will return ev.Offset + 1.
func (a *StreamAdaptor) HandleEvent(
	ctx context.Context,
	o uint64,
	ev eventstream.Event,
) (err error) {
	ak := ev.Parcel.Envelope.GetSourceApplication().GetKey()

	defer mlog.LogHandlerResult(
		a.Logger,
		ev.Parcel.Envelope,
		a.Identity,
		configkit.ProjectionHandlerType,
		&err,
		"resource %s, expected version: %d, new version: %d",
		ak,
		o,
		ev.Offset+1,
	)

	ctx, cancel := linger.ContextWithTimeout(
		ctx,
		a.Timeout,
		DefaultTimeout,
	)
	defer cancel()

	cp, err := a.Handler.HandleEvent(
		ctx,
		eventScope{
			cause:      ev.Parcel,
			checkpoint: o,
			offset:     ev.Offset,
			logger:     a.Logger,
		},
		ev.Parcel.Message.(dogma.Event),
	)
	if err != nil {
		return err
	}

	if cp != ev.Offset+1 {
		return errors.New("optimistic concurrency conflict")
	}

	return nil
}
