package eventstream

import (
	"context"
	"errors"

	"github.com/dogmatiq/configkit/message"
	"github.com/dogmatiq/infix/envelope"
)

// A Stream is an ordered sequence of event messages.
type Stream interface {
	// MessageTypes returns the complete set of message types that may appear on
	// the stream.
	MessageTypes(ctx context.Context) (message.TypeCollection, error)

	// Open returns a cursor used to read events from this stream.
	//
	// offset is the position of the first event to read. The first event on a
	// stream is always at offset 0.
	//
	// types is the set of event types that should be returned by Cursor.Next().
	// Any other event types are ignored.
	Open(
		ctx context.Context,
		offset uint64,
		types message.TypeCollection,
	) (cur Cursor, err error)
}

// ErrCursorClosed is returned by Cursor.Next() and Close() if the
// stream is closed.
var ErrCursorClosed = errors.New("stream cursor is closed")

// A Cursor reads events from a stream.
//
// Cursors are not safe for concurrent use.
type Cursor interface {
	// Next returns the next relevant event in the stream.
	//
	// If the end of the stream is reached it blocks until a relevant event is
	// appended to the stream or ctx is canceled.
	//
	// If the stream is closed before or during a call to Next(), it returns
	// ErrCursorClosed.
	Next(ctx context.Context) (*Event, error)

	// Close stops the cursor.
	//
	// It returns ErrCursorClosed if the cursor is already closed.
	//
	// Any current or future calls to Next() return ErrCursorClosed.
	Close() error
}

// Event is a container for an event consumed from an event stream..
type Event struct {
	// Offset is the offset of the message on the stream.
	Offset uint64

	// Envelope contains the message and its meta-data.
	Envelope *envelope.Envelope
}

// Handler handles events consumed from a stream.
type Handler interface {
	// NextOffset returns the offset of the next event to be consumed from a
	// specific application's event stream.
	//
	// k is the identity key of the source application.
	NextOffset(ctx context.Context, k string) (uint64, error)

	// HandleEvent handles an event obtained from the event stream.
	//
	// o is the offset value returned by NextOffset(). On success, the next call
	// to NextOffset() will return e.Offset + 1.
	HandleEvent(ctx context.Context, o uint64, e *Event) error
}
