package networkstream

import (
	"context"

	"github.com/dogmatiq/configkit"
	"github.com/dogmatiq/configkit/message"
	"github.com/dogmatiq/enginekit/collections/sets"
	"github.com/dogmatiq/enginekit/marshaler"
	"github.com/dogmatiq/interopspec/eventstreamspec"
	"github.com/dogmatiq/verity/eventstream"
)

// Stream is an implementation of eventstream.Stream that consumes messages via
// the dogma.messaging.v1 EventStream gRPC API.
type Stream struct {
	// App is the identity of the application that owns the stream.
	App configkit.Identity

	// Client is the gRPC client used to query the event stream server.
	Client eventstreamspec.StreamAPIClient

	// Marshaler is used to marshal and unmarshal messages and message types.
	Marshaler marshaler.Marshaler

	// PreFetch specifies how many messages to pre-load into memory.
	PreFetch int
}

// Application returns the identity of the application that owns the stream.
func (s *Stream) Application() configkit.Identity {
	return s.App
}

// EventTypes returns the set of event types that may appear on the stream.
func (s *Stream) EventTypes(ctx context.Context) (*sets.Set[message.Type], error) {
	req := &eventstreamspec.EventTypesRequest{
		ApplicationKey: s.App.Key,
	}

	res, err := s.Client.EventTypes(ctx, req)
	if err != nil {
		return nil, err
	}

	// Build a type-set containing any message types supported both by the
	// server and the client.
	types := sets.New[message.Type]()
	for _, t := range res.GetEventTypes() {
		rt, err := s.Marshaler.UnmarshalType(t.GetPortableName())
		if err == nil {
			types.Add(message.TypeFromReflect(rt))
		}
	}

	return types, nil
}

// Open returns a cursor that reads events from the stream.
//
// o is the offset of the first event to read. The first event on a stream is
// always at offset 0.
//
// f is the set of "filter" event types to be returned by Cursor.Next(). Any
// other event types are ignored.
//
// It returns an error if any of the event types in f are not supported, as
// indicated by EventTypes().
func (s *Stream) Open(
	ctx context.Context,
	o uint64,
	f *sets.Set[message.Type],
) (eventstream.Cursor, error) {
	req := s.buildConsumeRequest(o, f)

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

	stream, err := s.Client.Consume(consumeCtx, req)

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

		// If no error occurred, we hand ownership of cancelConsume() over to the
		// cursor to be called when the cursor is closed by the user.
		c := &cursor{
			stream:    stream,
			marshaler: s.Marshaler,
			cancel:    cancelConsume,
			events:    make(chan eventstream.Event, s.PreFetch),
		}

		go c.consume()

		return c, nil
	}
}

// buildConsumeRequest returns the request used to start consuming from the
// stream.
func (s *Stream) buildConsumeRequest(
	o uint64,
	f *sets.Set[message.Type],
) *eventstreamspec.ConsumeRequest {
	if f.Len() == 0 {
		panic("at least one event type must be specified")
	}

	req := &eventstreamspec.ConsumeRequest{
		ApplicationKey: s.App.Key,
		StartPoint: &eventstreamspec.ConsumeRequest_Offset{
			Offset: o,
		},
	}

	for mt := range f.All() {
		rt := mt.ReflectType()

		n, err := s.Marshaler.MarshalType(rt)
		if err != nil {
			panic(err)
		}

		req.EventTypes = append(
			req.EventTypes,
			&eventstreamspec.EventType{
				PortableName: n,
				MediaTypes:   s.Marshaler.MediaTypesFor(rt),
			},
		)
	}

	return req
}
