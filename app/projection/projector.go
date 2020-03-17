package projection

import (
	"context"
	"errors"
	"time"

	"github.com/dogmatiq/configkit"
	"github.com/dogmatiq/configkit/message"
	"github.com/dogmatiq/dodeca/logging"
	"github.com/dogmatiq/infix/app/projection/resource"
	"github.com/dogmatiq/infix/persistence"
	"github.com/dogmatiq/linger"
	"github.com/dogmatiq/linger/backoff"
)

// DefaultTimeout is the default timeout to use when applying an event.
const DefaultTimeout = 3 * time.Second

// Projector reads events from a stream and applies them to a projection.
type Projector struct {
	// SourceApplicationKey is the identity key of the application that produces
	// the events.
	SourceApplicationKey string

	// Stream is the source application's event stream.
	Stream persistence.Stream

	// ProjectionConfig is the configuration of the projection that the events
	// are applied to.
	ProjectionConfig configkit.RichProjection

	// DefaultTimeout is the timeout duration to use when hanlding an event if
	// the handler does not provide a timeout hint. If it is nil, DefaultTimeout
	// is used.
	DefaultTimeout time.Duration

	// BackoffStrategy is the strategy used to restarting the projector after a
	// failed call to consume, or message handling failure. If it is nil,
	// backoff.DefaultStrategy is used.
	BackoffStrategy backoff.Strategy

	// Logger is the target for log messages from the projector and the handler.
	// If it is nil, logging.DefaultLogger is used.
	Logger logging.Logger

	resource      []byte
	current       []byte
	next          []byte
	backoff       backoff.Counter
	handlerFailed bool
}

// Run consumes events from the stream and applies them to the projection until
// ctx is canceled or no more relevant events will occur.
func (p *Projector) Run(ctx context.Context) error {
	p.resource = resource.FromApplicationKey(p.SourceApplicationKey)
	p.next = make([]byte, 8)
	p.backoff = backoff.Counter{
		Strategy: p.BackoffStrategy,
	}

	for {
		err := p.consume(ctx)
		if err == nil {
			return nil
		}

		if ctx.Err() != nil {
			return ctx.Err()
		}

		logging.LogString(
			p.Logger,
			err.Error(),
		)

		if err := p.backoff.Sleep(ctx, err); err != nil {
			return err
		}
	}
}

// consume opens the stream, consumes messages ands applies them to the
// projection.
//
// It consumes until ctx is canceled, an error occurs, no more relevant events
// will occur.
func (p *Projector) consume(ctx context.Context) error {
	produced, err := p.Stream.MessageTypes(ctx)
	if err != nil {
		return err
	}

	consumed := p.ProjectionConfig.MessageTypes().Consumed
	relevant := message.IntersectionT(consumed, produced)

	if len(relevant) == 0 {
		logging.Debug(
			p.Logger,
			"stream does not produce any relevant event types",
		)

		return nil
	}

	for t := range relevant {
		logging.Debug(
			p.Logger,
			"consuming '%s' events",
			t,
		)
	}

	cur, err := p.open(ctx, relevant)
	if err != nil {
		return err
	}
	defer cur.Close()

	for {
		if err := p.consumeNext(ctx, cur); err != nil {
			return err
		}
	}
}

// open opens a cursor on the stream based on the offset recorded within the
// projection.
func (p *Projector) open(
	ctx context.Context,
	types message.TypeSet,
) (persistence.StreamCursor, error) {
	var (
		offset uint64
		err    error
	)

	p.current, err = p.ProjectionConfig.Handler().ResourceVersion(ctx, p.resource)
	if err != nil {
		return nil, err
	}

	offset, err = resource.UnmarshalOffset(p.current)
	if err != nil {
		return nil, err
	}

	logging.Log(
		p.Logger,
		"consuming %d event type(s), beginning at offset %d",
		len(types),
		offset,
	)

	return p.Stream.Open(ctx, offset, types)
}

// consumeNext waits for the next message on the stream then applies it to the
// projection.
func (p *Projector) consumeNext(ctx context.Context, cur persistence.StreamCursor) error {
	m, err := cur.Next(ctx)
	if err != nil {
		return err
	}

	// We've successfully obtained a message from the stream. If the last
	// failure was caused by the stream (and not the handler), reset the failure
	// count now, otherwise only reset it once we manage to actually apply the
	// event.
	if !p.handlerFailed {
		p.handlerFailed = false
		p.backoff.Reset()
	}

	resource.MarshalOffsetInto(p.next, m.Offset+1)

	h := p.ProjectionConfig.Handler()

	ctx, cancel := linger.ContextWithTimeout(
		ctx,
		h.TimeoutHint(m.Envelope.Message),
		p.DefaultTimeout,
		DefaultTimeout,
	)
	defer cancel()

	ok, err := h.HandleEvent(
		ctx,
		p.resource,
		p.current,
		p.next,
		scope{
			recordedAt: m.Envelope.CreatedAt,
			logger:     p.Logger,
		},
		m.Envelope.Message,
	)

	if ok {
		// keep swapping between the two buffers to avoid repeat allocations
		p.current, p.next = p.next, p.current
		p.backoff.Reset()

		return nil
	}

	p.handlerFailed = true

	if err != nil {
		return err
	}

	return errors.New("optimistic concurrency conflict")
}
