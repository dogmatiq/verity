package eventstream

import (
	"context"
	"time"

	"github.com/dogmatiq/aperture/ordered"
	"github.com/dogmatiq/configkit/message"
	"github.com/dogmatiq/dodeca/logging"
	"github.com/dogmatiq/dogma"
	"github.com/dogmatiq/linger"
	"go.opentelemetry.io/otel/api/metric"
	"go.opentelemetry.io/otel/api/trace"
)

// DefaultRetryTimeout is the default time to wait before restarting a
// failed projector.
const DefaultRetryTimeout = 10 * time.Second

// Projector provides a Supervisor entry-point that feeds events from a stream
// to a projection message handler.
type Projector struct {
	// Handler is the Dogma projection handler that the messages are applied to.
	Handler dogma.ProjectionMessageHandler

	// Logger is the target for log messages from the projector and the handler.
	// If it is nil, logging.DefaultLogger is used.
	Logger logging.Logger

	// DefaultTimeout is the timeout duration to use when hanlding an event if
	// the handler does not provide a timeout hint. If it is zero the global
	// DefaultTimeout constant is used.
	DefaultTimeout time.Duration

	// Meter is used to record metrics. If it is nil no metrics are recorded.
	Meter metric.Meter

	// Tracer is used to record tracing spans. If it is nil, no tracing is
	// performed.
	Tracer trace.Tracer

	// RetryTimeout is the time to wait before restarting a failed projector. If
	// it is 0, DefaultApertureRetryTimeout. Full jitter is applied.
	RetryTimeout time.Duration
}

// Run runs a projector for the given stream until ctx is canceled.
// It conforms to the EntryPoint signature used by Supervisor.
func (p *Projector) Run(ctx context.Context, str Stream) {
	op := &ordered.Projector{
		Stream:         &apertureStreamAdaptor{str},
		Handler:        p.Handler,
		Logger:         p.Logger,
		DefaultTimeout: p.DefaultTimeout,
		Tracer:         p.Tracer,
	}

	if p.Meter != nil {
		// TODO: setup op.Metrics
	}

	for {
		if ctx.Err() != nil {
			return
		}

		op.Run(ctx)

		linger.SleepX(
			ctx,
			linger.FullJitter,
			p.RetryTimeout,
			DefaultRetryTimeout,
		)
	}
}

// apertureStreamAdaptor adapts a Stream to the Aperture ordered.Stream
// interface.
type apertureStreamAdaptor struct {
	Stream Stream
}

// ID returns a unique identifier for the stream.
//
// The tuple of stream ID and event offset must uniquely identify a message.
func (a *apertureStreamAdaptor) ID() string {
	return a.Stream.Application().Key
}

// Open returns a cursor used to read events from this stream.
//
// offset is the position of the first event to read. The first event on a
// stream is always at offset 0. If the given offset is beyond the end of a
// sealed stream, ErrStreamSealed is returned.
//
// filter is a set of zero-value event messages, the types of which indicate
// which event types are returned by Cursor.Next(). If filter is empty, all
// events types are returned.
func (a *apertureStreamAdaptor) Open(
	ctx context.Context,
	offset uint64,
	filter []dogma.Message,
) (ordered.Cursor, error) {
	if len(filter) == 0 {
		panic("empty filter is not supported")
	}

	cur, err := a.Stream.Open(ctx, offset, message.TypesOf(filter...))
	if err != nil {
		return nil, err
	}

	return &apertureCursorAdaptor{cur}, nil
}

// apertureStreamAdaptor adapts a Stream to the Aperture ordered.Stream
// interface.
type apertureCursorAdaptor struct {
	Cursor Cursor
}

// Next returns the next relevant event in the stream.
//
// If the end of the stream is reached it blocks until a relevant event is
// appended to the stream, ctx is canceled or the stream is sealed. If the
// stream is sealed, ErrStreamSealed is returned.
func (a *apertureCursorAdaptor) Next(ctx context.Context) (ordered.Envelope, error) {
	env, err := a.Cursor.Next(ctx)

	return ordered.Envelope{
		Offset:     env.StreamOffset,
		RecordedAt: env.CreatedAt,
		Message:    env.Message,
	}, err
}

// Close stops the cursor.
//
// Any current or future calls to Next() return a non-nil error.
func (a *apertureCursorAdaptor) Close() error {
	return a.Cursor.Close()
}
