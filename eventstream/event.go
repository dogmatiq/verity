package eventstream

import "github.com/dogmatiq/infix/envelope"

// Offset is the 0-based index of an event on a stream.
type Offset uint64

// Event is a container for an event consumed from an event stream..
type Event struct {
	// Offset is the offset of the message on the stream.
	Offset Offset

	// Envelope contains the message and its meta-data.
	Envelope *envelope.Envelope
}
