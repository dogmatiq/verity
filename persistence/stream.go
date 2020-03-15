package persistence

import (
	"context"
	"errors"

	"github.com/dogmatiq/configkit/message"
	"github.com/dogmatiq/infix/envelope"
)

// ErrStreamCursorClosed is returned by StreamCursor.Next() and Close() if the
// stream is closed.
var ErrStreamCursorClosed = errors.New("stream cursor is closed")

// A Stream is an ordered sequence of messages.
type Stream interface {
	// Open returns a cursor used to read messages from this stream.
	//
	// offset is the position of the first message to read. The first message on
	// a stream is always at offset 0.
	//
	// types is a set of message types indicating which message types are
	// returned by Cursor.Next().
	Open(
		ctx context.Context,
		offset uint64,
		types message.TypeCollection,
	) (StreamCursor, error)

	// MessageTypes returns the message types that may appear on the stream.
	MessageTypes() message.TypeCollection
}

// A StreamCursor reads messages from a stream.
//
// There is no guarantee that cursors are safe for concurrent use.
type StreamCursor interface {
	// Next returns the next relevant message in the stream.
	//
	// If the end of the stream is reached it blocks until a relevant message is
	// appended to the stream or ctx is canceled.
	//
	// If the stream is closed before or during a call to Next(), it returns
	// ErrStreamCursorClosed.
	Next(ctx context.Context) (*StreamMessage, error)

	// Close stops the cursor.
	//
	// Any current or future calls to Next() return ErrStreamCursorClosed.
	Close() error
}

// StreamMessage is a message on a stream.
type StreamMessage struct {
	// Offset is the offset of the message on the stream.
	Offset uint64

	// Envelope contains the message and its meta-data.
	Envelope *envelope.Envelope
}
