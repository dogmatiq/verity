package eventstream

import (
	"context"

	"github.com/dogmatiq/configkit"
	"github.com/dogmatiq/configkit/message"
	imessage "github.com/dogmatiq/infix/message"
)

// A Stream is an ordered sequence of event messages.
type Stream interface {
	// Application returns the identity of the application that owns the stream.
	Application() configkit.Identity

	// Open returns a cursor used to read events from this stream.
	//
	// offset is the position of the first event to read. The first event
	// on a stream is always at offset 0.
	//
	// filter is a set of messages types of which indicate which event types are
	// returned by Cursor.Next().
	Open(
		ctx context.Context,
		offset uint64,
		filter message.TypeCollection,
	) (Cursor, error)
}

// A Cursor reads events from a stream.
//
// Cursors are not intended to be used by multiple goroutines concurrently.
type Cursor interface {
	// Next returns the next relevant event in the stream.
	//
	// filter is a set of message types that indicates which event types are
	// returned by Cursor.Next(). If filter is empty, all events types are returned.
	Next(ctx context.Context) (*Envelope, error)

	// Close stops the cursor.
	//
	// Any current or future calls to Next() return a non-nil error.
	Close() error
}

// Envelope is a specialized envelope for messages consumed from a stream.
type Envelope struct {
	*imessage.Envelope

	// StreamOffset is the offset of the message on the stream.
	StreamOffset uint64
}
