package eventstream

import (
	"context"
	"errors"

	"github.com/dogmatiq/configkit"
	"github.com/dogmatiq/enginekit/collections/sets"
	"github.com/dogmatiq/enginekit/message"
)

// A Stream is an ordered sequence of event messages.
type Stream interface {
	// Application returns the identity of the application that owns the stream.
	Application() configkit.Identity

	// EventTypes returns the set of event types that may appear on the stream.
	EventTypes(ctx context.Context) (*sets.Set[message.Type], error)

	// Open returns a cursor that reads events from the stream.
	//
	// o is the offset of the first event to read. The first event on a stream
	// is always at offset 0.
	//
	// f is the set of "filter" event types to be returned by Cursor.Next(). Any
	// other event types are ignored.
	//
	// It returns an error if any of the event types in f are not supported, as
	// indicated by EventTypes().
	Open(ctx context.Context, o uint64, f *sets.Set[message.Type]) (Cursor, error)
}

var (
	// ErrCursorClosed is returned by Cursor.Next() and Close() if the
	// stream is closed.
	ErrCursorClosed = errors.New("stream cursor is closed")

	// ErrTruncated indicates that a cursor can not be opened because the requested
	// offset is on a portion of the event stream that has been truncated.
	ErrTruncated = errors.New("can not open cursor, stream is truncated")
)

// A Cursor reads events from a stream.
//
// Cursors are not safe for concurrent use.
type Cursor interface {
	// Next returns the next event in the stream that matches the filter.
	//
	// If the end of the stream is reached it blocks until a relevant event is
	// appended to the stream or ctx is canceled.
	//
	// If the stream is closed before or during a call to Next(), it returns
	// ErrCursorClosed.
	//
	// It returns ErrTruncated if the next event can not be obtained because it
	// occupies a portion of the stream that has been truncated.
	Next(ctx context.Context) (Event, error)

	// Close discards the cursor.
	//
	// It returns ErrCursorClosed if the cursor is already closed.
	// Any current or future calls to Next() return ErrCursorClosed.
	Close() error
}
