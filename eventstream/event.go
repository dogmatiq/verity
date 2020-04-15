package eventstream

import "github.com/dogmatiq/infix/parcel"

// Offset is the 0-based index of an event on a stream.
type Offset uint64

// Event is a container for an envelope and event stream specific meta-data.
type Event struct {
	// Offset is the offset of the message on the stream.
	Offset Offset

	// Parcel contains the event from the stream.
	Parcel *parcel.Parcel
}
