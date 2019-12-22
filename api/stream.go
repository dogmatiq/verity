package api

import (
	"context"

	"github.com/dogmatiq/configkit"
	"github.com/dogmatiq/configkit/message"
	"github.com/dogmatiq/infix/api/internal/pb"
	"github.com/dogmatiq/infix/eventstream"
	"github.com/dogmatiq/marshalkit"
)

// Stream is an implementation of eventstream.Stream that obtains messages via
// the messaging API.
type Stream struct {
	// Application is the identity of the app that the stream consumes from.
	Application configkit.Identity

	// Marshaler is used to marshal messages and their types.
	Marshaler marshalkit.Marshaler

	// Client is the underlying gRPC event stream.
	Client pb.EventStreamClient
}

// ID returns a unique identifier for the stream.
//
// The tuple of stream ID and event offset must uniquely identify a message.
func (s *Stream) ID() string {
	return s.Application.Key
}

// Open returns a cursor used to read events from this stream.
//
// offset is the position of the first event to read. The first event
// on a stream is always at offset 0.
//
// filter is a set of message types that indicates which event types are
// returned by Cursor.Next(). If filter is empty, all events types are returned.
func (s *Stream) Open(
	ctx context.Context,
	offset uint64,
	filter message.TypeCollection,
) (eventstream.Cursor, error) {
	req := &pb.ConsumeRequest{
		Application: &pb.Identity{
			Name: s.Application.Name,
			Key:  s.Application.Key,
		},
		Offset: offset,
		Events: s.marshalFilter(filter),
	}

	// Create a new context for the lifetime of the cursor. Note that it is NOT
	// a child of ctx, because ctx is only intended to control the lifetime of
	// the call to Open().
	consumeCtx, cancel := context.WithCancel(context.Background())

	// Rig up a goroutine to cancel consumeCtx if ctx is canceled while the call
	// to gRPC's Consume() method is in-progress.
	//
	// The done channel is used to abort this goroutine once the call to
	// Consume() has returned.
	done := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			cancel()
		case <-done:
		}
	}()

	// Start the gRPC consumer.
	res, err := s.Client.Consume(consumeCtx, req)

	// Abort the waiting goroutine so that consumeCtx will no longer be canceled
	// when and if ctx is canceled.
	close(done)

	if err != nil {
		cancel() // cancel consumeCtx to avoid a leak
		return nil, err
	}

	cur := &cursor{
		marshaler: s.Marshaler,
		cancel:    cancel, // capture the cancel function for cursorCtx
		client:    res,
		next:      make(chan *eventstream.Envelope),
	}

	// start piping messages from the gRPC stream to cur.next.
	go cur.consume(consumeCtx)

	return cur, nil
}

// marshalFilter returns the media types for each of the given message types.
//
// It panics if any if s.Marshaler can not marshal any of the types.
func (s *Stream) marshalFilter(types message.TypeCollection) []string {
	var result []string

	types.Each(
		func(t message.Type) bool {
			mt := marshalkit.MustMarshalType(s.Marshaler, t.ReflectType())
			result = append(result, mt)
			return true
		},
	)

	return result
}

// cursor is an implementation of eventstream.Cursor that reads messages from
type cursor struct {
	marshaler marshalkit.ValueMarshaler
	cancel    func()
	client    pb.EventStream_ConsumeClient
	next      chan *eventstream.Envelope
	err       error
}

// Next returns the next relevant event in the stream.
//
// If the end of the stream is reached, it blocks until a relevant event
// is appended to the stream, or ctx is canceled.
func (c *cursor) Next(ctx context.Context) (*eventstream.Envelope, error) {
	select {
	case env, ok := <-c.next:
		if ok {
			return env, nil
		}
		return nil, c.err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// Close stops the cursor.
//
// Any current or future calls to Next() return a non-nil error.
func (c *cursor) Close() error {
	c.cancel()
	return nil
}

// consume reads envelopes from gRPC and writes them to c.next until ctx is
// canceled or an error occurs.
func (c *cursor) consume(ctx context.Context) {
	defer close(c.next)

	for c.err == nil {
		c.err = c.tick(ctx)
	}
}

// tick reads the next message from the gRPC stream and writes it to c.next.
func (c *cursor) tick(ctx context.Context) error {
	res, err := c.client.Recv()
	if err != nil {
		return err
	}

	env := &eventstream.Envelope{
		// TODO: Envelope
		StreamOffset: res.Offset,
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	case c.next <- env:
		return nil
	}
}
