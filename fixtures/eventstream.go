package fixtures

import (
	"context"

	"github.com/dogmatiq/enginekit/collections/sets"
	"github.com/dogmatiq/enginekit/message"
	"github.com/dogmatiq/enginekit/protobuf/identitypb"
	"github.com/dogmatiq/verity/eventstream"
)

// EventStreamStub is a test implementation of the eventstream.Stream interface.
type EventStreamStub struct {
	eventstream.Stream

	ApplicationFunc func() *identitypb.Identity
	EventTypesFunc  func(context.Context) (*sets.Set[message.Type], error)
	OpenFunc        func(context.Context, uint64, *sets.Set[message.Type]) (eventstream.Cursor, error)
}

// Application returns the identity of the application that owns the stream.
func (s *EventStreamStub) Application() *identitypb.Identity {
	if s.ApplicationFunc != nil {
		return s.ApplicationFunc()
	}

	if s.Stream != nil {
		return s.Stream.Application()
	}

	return nil
}

// EventTypes returns the set of event types that may appear on the stream.
func (s *EventStreamStub) EventTypes(ctx context.Context) (*sets.Set[message.Type], error) {
	if s.EventTypesFunc != nil {
		return s.EventTypesFunc(ctx)
	}

	if s.Stream != nil {
		return s.Stream.EventTypes(ctx)
	}

	return nil, nil
}

// Open returns a cursor that reads events from the stream.
func (s *EventStreamStub) Open(
	ctx context.Context,
	offset uint64,
	types *sets.Set[message.Type],
) (eventstream.Cursor, error) {
	if s.OpenFunc != nil {
		return s.OpenFunc(ctx, offset, types)
	}

	if s.Stream != nil {
		return s.Stream.Open(ctx, offset, types)
	}

	return nil, nil
}

// EventStreamHandlerStub is a test implementation of the eventstream.Handler
// interface.
type EventStreamHandlerStub struct {
	eventstream.Handler

	NextOffsetFunc  func(context.Context, *identitypb.Identity) (uint64, error)
	HandleEventFunc func(context.Context, uint64, eventstream.Event) error
}

// NextOffset returns the offset of the next event to be consumed from a
// specific application's event stream.
func (h *EventStreamHandlerStub) NextOffset(
	ctx context.Context,
	id *identitypb.Identity,
) (uint64, error) {
	if h.NextOffsetFunc != nil {
		return h.NextOffsetFunc(ctx, id)
	}

	if h.Handler != nil {
		return h.Handler.NextOffset(ctx, id)
	}

	return 0, nil
}

// HandleEvent handles an event obtained from the event stream.
func (h *EventStreamHandlerStub) HandleEvent(
	ctx context.Context,
	o uint64,
	ev eventstream.Event,
) error {
	if h.HandleEventFunc != nil {
		return h.HandleEventFunc(ctx, o, ev)
	}

	if h.Handler != nil {
		return h.Handler.HandleEvent(ctx, o, ev)
	}

	return nil
}
