package eventstream

import (
	"context"
	"sync"

	"github.com/dogmatiq/configkit/message"
	"github.com/dogmatiq/infix/envelope"
	"github.com/dogmatiq/infix/eventstream"
	"github.com/dogmatiq/infix/internal/draftspecs/messagingspec"
	"github.com/dogmatiq/marshalkit"
	"google.golang.org/grpc"
)

// NewEventStream returns a stream that consumes events from a specific source
// application.
//
// k is the identity key of the application to consume from. c a connection to
// a server that both implements the EventStream service, and hosts the
// specified application.
//
// m is the marshaler used to marshal and unmarshal events. n the "pre-fetch"
// count, which is the number of events to buffer in memory on the client side.
func NewEventStream(
	k string,
	c *grpc.ClientConn,
	m marshalkit.Marshaler,
	n int,
) eventstream.Stream {
	return &stream{
		appKey:    k,
		client:    messagingspec.NewEventStreamClient(c),
		marshaler: m,
		prefetch:  n,
	}
}

// stream is an implementation of eventstream.Stream that consumes messages via
// the EventStream gRPC API.
type stream struct {
	appKey    string
	client    messagingspec.EventStreamClient
	marshaler marshalkit.Marshaler
	prefetch  int
}

// ApplicationKey returns the identity key of the application that owns the
// stream.
func (s *stream) ApplicationKey() string {
	return s.appKey
}

// Open returns a cursor used to read events from this stream.
//
// offset is the position of the first event to read. The first event on a
// stream is always at offset 0.
//
// types is the set of event types that should be returned by Cursor.Next().
// Any other event types are ignored.
func (s *stream) Open(
	ctx context.Context,
	offset uint64,
	types message.TypeCollection,
) (eventstream.Cursor, error) {
	// consumeCtx lives for the lifetime of the stream returned by the gRPC
	// Consume() operation. This is how we cancel the gRPC stream, as it has no
	// Close() method.
	//
	// It does NOT use ctx as a parent, because ctx is intended only to span the
	// lifetime of this call to Open().
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

	req := &messagingspec.ConsumeRequest{
		ApplicationKey: s.appKey,
		Offset:         offset,
		Types:          marshalMessageTypes(s.marshaler, types),
	}

	stream, err := s.client.Consume(consumeCtx, req)

	select {
	case <-ctx.Done():
		// If the ctx closed while we were inside Consume() it doesn't really
		// matter what the result is. cancelConsume() will be called by the
		// goroutine above.
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
			stream:    stream,
			marshaler: s.marshaler,
			cancel:    cancelConsume,
			events:    make(chan *eventstream.Event, s.prefetch),
		}

		go c.consume()

		return c, nil
	}
}

// MessageTypes returns the complete set of event types that may appear on
// the stream.
func (s *stream) MessageTypes(ctx context.Context) (message.TypeCollection, error) {
	req := &messagingspec.MessageTypesRequest{
		ApplicationKey: s.appKey,
	}

	res, err := s.client.MessageTypes(ctx, req)
	if err != nil {
		return nil, err
	}

	// Build a type-set containing any message types supported by the remote end
	// that we know how to unmarshal on this end.
	types := message.TypeSet{}
	for _, t := range res.GetMessageTypes() {
		rt, err := s.marshaler.UnmarshalType(t.PortableName)
		if err == nil {
			types.Add(message.TypeFromReflect(rt))
		}
	}

	return types, nil
}

// cursor is an implementation of eventstream.Cursor that consumes events via
// the EventStream gRPC API.
type cursor struct {
	stream    messagingspec.EventStream_ConsumeClient
	marshaler marshalkit.ValueMarshaler
	once      sync.Once
	cancel    context.CancelFunc
	events    chan *eventstream.Event
	err       error
}

// Next returns the next relevant event in the stream.
//
// If the end of the stream is reached it blocks until a relevant event is
// appended to the stream or ctx is canceled.
//
// If the stream is closed before or during a call to Next(), it returns
// ErrCursorClosed.
func (c *cursor) Next(ctx context.Context) (*eventstream.Event, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case ev, ok := <-c.events:
		if ok {
			return ev, nil
		}

		return nil, c.err
	}
}

// Close stops the cursor.
//
// It returns ErrCursorClosed if the cursor is already closed.
//
// Any current or future calls to Next() return ErrCursorClosed.
func (c *cursor) Close() error {
	if !c.close(eventstream.ErrCursorClosed) {
		return eventstream.ErrCursorClosed
	}

	return nil
}

// consume receives new events, unmarshals them, and pipes them over the
// c.events channel to a goroutine that calls Next().
//
// It exits when the context associated with c.stream is canceled or some other
// error occurs while reading from the stream.
func (c *cursor) consume() {
	defer close(c.events)

	for {
		err := c.recv()

		if err != nil {
			c.close(err)
			return
		}
	}
}

// close closes the cursor. It returns false if the cursor was already closed.
func (c *cursor) close(cause error) bool {
	ok := false

	c.once.Do(func() {
		c.cancel()
		c.err = cause
		ok = true
	})

	return ok
}

// recv waits for the next event from the stream, unmarshals it and sends it
// over the c.events channel.
func (c *cursor) recv() error {
	// We can't pass ctx to Recv(), but the stream is already bound to a context.
	res, err := c.stream.Recv()
	if err != nil {
		return err
	}

	ev := &eventstream.Event{
		Offset:   res.Offset,
		Envelope: &envelope.Envelope{},
	}

	if err := envelope.Unmarshal(
		c.marshaler,
		res.GetEnvelope(),
		ev.Envelope,
	); err != nil {
		return err
	}

	select {
	case c.events <- ev:
		return nil
	case <-c.stream.Context().Done():
		return c.stream.Context().Err()
	}
}

// marshalMessageTypes marshals a collection of message types to their protocol
// buffers representation.
func marshalMessageTypes(
	m marshalkit.TypeMarshaler,
	in message.TypeCollection,
) []string {
	var out []string

	in.Range(func(t message.Type) bool {
		out = append(
			out,
			marshalkit.MustMarshalType(m, t.ReflectType()),
		)
		return true
	})

	return out
}
