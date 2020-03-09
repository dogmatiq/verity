package api

import (
	"context"
	"fmt"

	"github.com/dogmatiq/configkit"
	"github.com/dogmatiq/configkit/message"
	"github.com/dogmatiq/infix/api/internal/pb"
	"github.com/dogmatiq/infix/envelope"
	"github.com/dogmatiq/infix/eventstream"
	"github.com/dogmatiq/marshalkit"
	"google.golang.org/grpc"
)

// stream is an implementation of eventstream.Stream that consumes messages via
// the EventStream gRPC API.
type stream struct {
	appKey    string
	client    pb.EventStreamClient
	marshaler marshalkit.Marshaler
	prefetch  int
}

// NewStream returns a stream that consumes events from a specific source
// application.
//
// id is the identity key of the application to consume from. c a connection to
// a server that both implements the EventStream service, and hosts the
// specified application.
//
// m is the marshaler used to marshal and unmarshal messages. n the "pre-fetch"
// count, which is the number of messages to buffer in memory on the before they
// are requested by a call to Cursor.Next().
func NewStream(
	id configkit.Identity,
	c *grpc.ClientConn,
	m marshalkit.Marshaler,
	n int,
) eventstream.Stream {
	return &stream{
		appKey:    id.Key,
		client:    pb.NewEventStreamClient(c),
		marshaler: m,
		prefetch:  n,
	}
}

func (s *stream) Open(
	ctx context.Context,
	offset uint64,
	types []message.Type,
) (eventstream.Cursor, error) {
	req, err := s.newRequest(offset, types)
	if err != nil {
		return nil, err
	}

	// consumeCtx lives for the lifetime of the stream returned by the gRPC
	// Consume() operation. This is how we cancel the gRPC stream, as it has no
	// Close() method.
	//
	// It does NOT use ctx as a parent, because ctx is intended only to span
	// the lifetime of this call to Open().
	consumeCtx, cancelConsume := context.WithCancel(context.Background())

	// done is signalling channel used to indicate that the call to Consume()
	// has returned successfully.
	done := make(chan struct{})

	// This goroutine aborts a pending call to Consume() if ctx is canceled
	// before it returns.
	go func() {
		select {
		case <-ctx.Done():
			cancelConsume()
		case <-done:
		}
	}()

	stream, err := s.client.Consume(consumeCtx, req)

	select {
	case <-ctx.Done():
		// If the ctx closed while we were inside Consume() it doesn't really
		// matter what the result is. cancelConsume() will be called by
		// the goroutine above.
		return nil, ctx.Err()

	case done <- struct{}{}:
		// If we manage to send a value to the goroutine above, we have
		// guaranteed that it wont call cancelConsume(), so we are now
		// responsible for doing so.
		if err != nil {
			cancelConsume()
			return nil, err
		}

		// If no error occured, we hand ownership of cancelConsume() over to the
		// cursor to be called when the cursor is closed by the user.
		c := &cursor{
			marshaler: s.marshaler,
			stream:    stream,
			cancel:    cancelConsume,
			events:    make(chan *eventstream.Event, s.prefetch),
		}

		go c.consume(ctx)

		return c, nil
	}
}

// newRequest constructs a ConsumeRequest for the given offset and types.
func (s *stream) newRequest(
	offset uint64,
	types []message.Type,
) (*pb.ConsumeRequest, error) {
	req := &pb.ConsumeRequest{
		ApplicationKey: s.appKey,
		Offset:         offset,
	}

	for _, t := range types {
		n, err := s.marshaler.MarshalType(t.ReflectType())
		if err != nil {
			return nil, err
		}

		req.Types = append(req.Types, n)
	}

	return req, nil
}

// cursor is an implementation of eventstream.Cursor that consumes messages via
// the EventStream gRPC API.
type cursor struct {
	marshaler marshalkit.ValueMarshaler
	stream    pb.EventStream_ConsumeClient
	cancel    context.CancelFunc
	events    chan *eventstream.Event
	err       error
}

func (c *cursor) Next(ctx context.Context) (*eventstream.Event, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case e, _ := <-c.events:
		return e, fmt.Errorf("the cursor is closed: %w", c.err)
	}
}

func (c *cursor) Close() error {
	c.cancel()
	return nil
}

// consume waits receives new messages, unmarshals them and pipes them over the
// c.events channel to a goroutine that calls Next().
//
// It exits when the context associated with c.stream is canceled or some other
// error occurs while reading from the stream.
func (c *cursor) consume(ctx context.Context) {
	defer close(c.events)

	for c.err == nil {
		c.err = c.recv(ctx)
	}
}

// recv waits for the next message from the stream, unmarshals it and sends it
// over the c.events channel.
func (c *cursor) recv(ctx context.Context) error {
	// We can't pass ctx to Recv(), but the stream is already bound to ctx.
	res, err := c.stream.Recv()
	if err != nil {
		return err
	}

	e := &eventstream.Event{
		Offset:   res.Offset,
		Envelope: &envelope.Envelope{},
	}

	if err := unmarshalEnvelope(
		c.marshaler,
		res.GetEnvelope(),
		e.Envelope,
	); err != nil {
		return err
	}

	select {
	case c.events <- e:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
