package fixtures

import (
	"context"

	"github.com/dogmatiq/configkit"
	"github.com/dogmatiq/configkit/message"
	"github.com/dogmatiq/infix/eventstream"
	"github.com/dogmatiq/infix/persistence/provider/memory"
)

// Stream is a mock of the eventstream.Stream interface.
//
// It is based on a memory stream.
type Stream struct {
	Memory memory.Stream

	ApplicationFunc  func() configkit.Identity
	MessageTypesFunc func(context.Context) (message.TypeCollection, error)
	OpenFunc         func(context.Context, eventstream.Offset, message.TypeCollection) (eventstream.Cursor, error)
}

// Application returns the identity of the application that owns the stream.
//
// If s.ApplicationFunc is non-nil, it returns s.ApplicationFunc(), otherwise it
// dispatches to s.Memory.
func (s *Stream) Application() configkit.Identity {
	if s.ApplicationFunc != nil {
		return s.ApplicationFunc()
	}

	return s.Memory.Application()
}

// MessageTypes returns the complete set of event types that may appear on the
// stream.
//
// If s.MessageTypesFunc is non-nil, it returns s.MessageTypesFunc(ctx),
// otherwise it dispatches to s.Memory.
func (s *Stream) MessageTypes(ctx context.Context) (message.TypeCollection, error) {
	if s.MessageTypesFunc != nil {
		return s.MessageTypesFunc(ctx)
	}

	return s.Memory.MessageTypes(ctx)
}

// Open returns a cursor used to read events from this stream.
//
// If s.OpenFunc is non-nil, it returns s.OpenFunc(ctx, offset, types),
// otherwise it dispatches to s.Memory.
func (s *Stream) Open(
	ctx context.Context,
	offset eventstream.Offset,
	types message.TypeCollection,
) (eventstream.Cursor, error) {
	if s.OpenFunc != nil {
		return s.OpenFunc(ctx, offset, types)
	}

	return s.Memory.Open(ctx, offset, types)
}

// StreamHandler is a mock of the eventstream.Handler interface.
type StreamHandler struct {
	NextOffsetFunc  func(context.Context, configkit.Identity) (eventstream.Offset, error)
	HandleEventFunc func(context.Context, eventstream.Offset, *eventstream.Event) error
}

// NextOffset returns the offset of the next event to be consumed from a
// specific application's event stream.
//
// If h.NextOffsetFunc is non-nil, it returns h.NextOffsetFunc(ctx, id),
// otherwise it returns (0, nil).
func (h *StreamHandler) NextOffset(
	ctx context.Context,
	id configkit.Identity,
) (eventstream.Offset, error) {
	if h.NextOffsetFunc != nil {
		return h.NextOffsetFunc(ctx, id)
	}

	return 0, nil
}

// HandleEvent handles an event obtained from the event stream.
//
// If h.HandleEventFunc is non-nil, it returns h.HandleEventFunc(ctx, o, ev),
// otherwise it returns nil.
func (h *StreamHandler) HandleEvent(
	ctx context.Context,
	o eventstream.Offset,
	ev *eventstream.Event,
) error {
	if h.HandleEventFunc != nil {
		return h.HandleEventFunc(ctx, o, ev)
	}

	return nil
}
