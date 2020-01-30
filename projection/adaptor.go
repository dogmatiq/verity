package projection

import (
	"context"

	"github.com/dogmatiq/aperture/ordered"
	"github.com/dogmatiq/configkit/message"
	"github.com/dogmatiq/dogma"
	"github.com/dogmatiq/infix/eventstream"
)

// StreamAdaptor adapts an eventstream.Stream to the Aperture ordered.Stream
// interface.
type StreamAdaptor struct {
	Stream eventstream.Stream
}

// ID returns a unique identifier for the stream.
//
// The tuple of stream ID and event offset must uniquely identify a message.
func (a *StreamAdaptor) ID() string {
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
func (a *StreamAdaptor) Open(
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

	return &CursorAdaptor{cur}, nil
}

// CursorAdaptor adapts an eventstream.Cursor to the Aperture ordered.Stream
// interface.
type CursorAdaptor struct {
	Cursor eventstream.Cursor
}

// Next returns the next relevant event in the stream.
//
// If the end of the stream is reached it blocks until a relevant event is
// appended to the stream, ctx is canceled or the stream is sealed. If the
// stream is sealed, ErrStreamSealed is returned.
func (a *CursorAdaptor) Next(ctx context.Context) (ordered.Envelope, error) {
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
func (a *CursorAdaptor) Close() error {
	return a.Cursor.Close()
}
