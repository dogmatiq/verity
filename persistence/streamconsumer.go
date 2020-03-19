package persistence

import (
	"context"

	"github.com/dogmatiq/configkit/message"
	"github.com/dogmatiq/dodeca/logging"
	"github.com/dogmatiq/linger/backoff"
)

// StreamEventHandler handles events consumed from a stream.
type StreamEventHandler interface {
	// NextOffset returns the next offset to be consumed from the event stream.
	//
	// k is the identity key of the source application.
	NextOffset(ctx context.Context, k string) (uint64, error)

	// HandleEvent handles a message consumed from the event stream.
	//
	// o is the offset value returned by NextOffset().
	HandleEvent(ctx context.Context, o uint64, m *StreamMessage) error
}

// StreamConsumer handles events consumed from an application's event stream.
type StreamConsumer struct {
	// ApplicationKey is the identity key of the application that produces the
	// events.
	ApplicationKey string

	// Stream is the application's event stream stream.
	Stream Stream

	// Types is the set of types that the handler consumes.
	Types message.TypeCollection

	// Handler is the target for the messages from the stream.
	Handler StreamEventHandler

	// BackoffStrategy is the strategy used to delay restarting the consumer
	// after a failure. If it is nil, backoff.DefaultStrategy is used.
	BackoffStrategy backoff.Strategy

	// Logger is the target for log messages from the consumer.
	// If it is nil, logging.DefaultLogger is used.
	Logger logging.Logger

	offset        uint64
	backoff       backoff.Counter
	handlerFailed bool
}

// Run handles events from the stream until ctx is canceled or no more relevant
// events will occur.
func (c *StreamConsumer) Run(ctx context.Context) error {
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

// consume opens the stream and starts handling events.
//
// It consumes until ctx is canceled, an error occurs, no more relevant events
// will occur.
func (c *StreamConsumer) consume(ctx context.Context) error {
	produced, err := c.Stream.MessageTypes(ctx)
	if err != nil {
		return err
	}

	relevant := message.IntersectionT(c.Types, produced)

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

// open opens a stream cursor based on the offset given by the handler.
func (c *StreamConsumer) open(
	ctx context.Context,
	types message.TypeSet,
) (StreamCursor, error) {
	var err error
	c.offset, err = c.Handler.NextOffset(ctx, c.ApplicationKey)
	if err != nil {
		return nil, err
	}

	logging.Log(
		c.Logger,
		"consuming %d event type(s), beginning at offset %d",
		len(types),
		c.offset,
	)

	return c.Stream.Open(ctx, c.offset, types)
}

// consumeNext waits for the next message on the stream then handles it.
func (c *StreamConsumer) consumeNext(ctx context.Context, cur StreamCursor) error {
	m, err := cur.Next(ctx)
	if err != nil {
		return err
	}

	// We've successfully obtained a message from the stream. If the last
	// failure was caused by the stream (and not the handler), reset the failure
	// count now, otherwise only reset it once we manage to actually handle the
	// event.
	if !c.handlerFailed {
		c.handlerFailed = false
		c.backoff.Reset()
	}

	if err := c.Handler.HandleEvent(ctx, c.offset, m); err != nil {
		c.handlerFailed = true
		return err
	}

	c.offset = m.Offset + 1
	c.backoff.Reset()

	return nil
}
