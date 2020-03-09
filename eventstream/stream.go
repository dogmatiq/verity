package eventstream

import (
	"context"

	"github.com/dogmatiq/configkit/message"
	"github.com/dogmatiq/infix/envelope"
)

// A Stream is an ordered sequence of event messages.
type Stream interface {
	// Open returns a cursor used to read events from this stream.
	//
	// offset is the position of the first event to read. The first event on a
	// stream is always at offset 0.  is returned.
	//
	// types is a set of message types indicating which event types are returned
	// by Cursor.Next().
	Open(
		ctx context.Context,
		offset uint64,
		types message.TypeCollection,
	) (Cursor, error)
}

// A Cursor reads events from a stream.
//
// Cursors are not intended to be used by multiple goroutines concurrently.
type Cursor interface {
	// Next returns the next relevant event in the stream.
	//
	// If the end of the stream is reached it blocks until a relevant event is
	// appended to the stream or ctx is canceled.
	Next(ctx context.Context) (*Event, error)

	// Close stops the cursor.
	//
	// Any current or future calls to Next() return a non-nil error.
	Close() error
}

// Event is an event on an event stream.
type Event struct {
	// Offset is the offset of the event on the stream.
	Offset uint64

	// Envelope contains the event message and its meta-data.
	Envelope *envelope.Envelope
}
