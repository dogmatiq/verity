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

// StreamConsumer consumes events from a stream to be handled by a projection
// message handler.
type StreamConsumer struct {
	// ApplicationKey is the identity key of the application that produces the
	// events.
	ApplicationKey string

	// Stream is the source application's event stream.
	Stream persistence.Stream

	// ProjectionConfig is the configuration of the projection that handles the
	// events.
	ProjectionConfig configkit.RichProjection

	// DefaultTimeout is the maximum time to allow for handling a single event
	// if the handler does not provide a timeout hint. If it is nil,
	// DefaultTimeout is used.
	DefaultTimeout time.Duration

	// BackoffStrategy is the strategy used to delay restarting the consumer
	// after a failure. If it is nil, backoff.DefaultStrategy is used.
	BackoffStrategy backoff.Strategy

	// Logger is the target for log messages from the consumer and the handler.
	// If it is nil, logging.DefaultLogger is used.
	Logger logging.Logger

	resource      []byte
	current       []byte
	next          []byte
	backoff       backoff.Counter
	handlerFailed bool
}

// Run handles events from the stream until ctx is canceled or no more relevant
// events will occur.
func (c *StreamConsumer) Run(ctx context.Context) error {
	c.resource = resource.FromApplicationKey(c.ApplicationKey)
	c.backoff = backoff.Counter{
		Strategy: c.BackoffStrategy,
	}

	for {
		err := c.consume(ctx)
		if err == nil {
			return nil
		}

		if ctx.Err() != nil {
			return ctx.Err()
		}

		logging.LogString(
			c.Logger,
			err.Error(),
		)

		if err := c.backoff.Sleep(ctx, err); err != nil {
			return err
		}
	}
}

// consume opens the stream, consumes messages ands applies them to the
// projection.
//
// It consumes until ctx is canceled, an error occurs, no more relevant events
// will occur.
func (c *StreamConsumer) consume(ctx context.Context) error {
	produced, err := c.Stream.MessageTypes(ctx)
	if err != nil {
		return err
	}

	consumed := c.ProjectionConfig.MessageTypes().Consumed
	relevant := message.IntersectionT(consumed, produced)

	if len(relevant) == 0 {
		logging.Debug(
			c.Logger,
			"stream does not produce any relevant event types",
		)

		return nil
	}

	for t := range relevant {
		logging.Debug(
			c.Logger,
			"consuming '%s' events",
			t,
		)
	}

	cur, err := c.open(ctx, relevant)
	if err != nil {
		return err
	}
	defer cur.Close()

	for {
		if err := c.consumeNext(ctx, cur); err != nil {
			return err
		}
	}
}

// open opens a cursor on the stream based on the offset recorded within the
// projection.
func (c *StreamConsumer) open(
	ctx context.Context,
	types message.TypeSet,
) (persistence.StreamCursor, error) {
	var (
		offset uint64
		err    error
	)

	c.current, err = c.ProjectionConfig.Handler().ResourceVersion(ctx, c.resource)
	if err != nil {
		return nil, err
	}

	offset, err = resource.UnmarshalOffset(c.current)
	if err != nil {
		return nil, err
	}

	logging.Log(
		c.Logger,
		"consuming %d event type(s), beginning at offset %d",
		len(types),
		offset,
	)

	return c.Stream.Open(ctx, offset, types)
}

// consumeNext waits for the next message on the stream then applies it to the
// projection.
func (c *StreamConsumer) consumeNext(ctx context.Context, cur persistence.StreamCursor) error {
	m, err := cur.Next(ctx)
	if err != nil {
		return err
	}

	// We've successfully obtained a message from the stream. If the last
	// failure was caused by the stream (and not the handler), reset the failure
	// count now, otherwise only reset it once we manage to actually apply the
	// event.
	if !c.handlerFailed {
		c.handlerFailed = false
		c.backoff.Reset()
	}

	if c.next == nil {
		c.next = make([]byte, 8)
	}
	resource.MarshalOffsetInto(c.next, m.Offset+1)

	h := c.ProjectionConfig.Handler()

	ctx, cancel := linger.ContextWithTimeout(
		ctx,
		h.TimeoutHint(m.Envelope.Message),
		c.DefaultTimeout,
		DefaultTimeout,
	)
	defer cancel()

	ok, err := h.HandleEvent(
		ctx,
		c.resource,
		c.current,
		c.next,
		scope{
			recordedAt: m.Envelope.CreatedAt,
			logger:     c.Logger,
		},
		m.Envelope.Message,
	)

	if ok {
		// keep swapping between the two buffers to avoid repeat allocations
		c.current, c.next = c.next, c.current
		c.backoff.Reset()

		return nil
	}

	c.handlerFailed = true

	if err != nil {
		return err
	}

	return errors.New("optimistic concurrency conflict")
}
