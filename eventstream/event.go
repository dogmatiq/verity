package eventstream

import "github.com/dogmatiq/verity/parcel"

// Event is a container for an envelope and event stream specific meta-data.
type Event struct {
	// Offset is the 0-based index of the event on the stream.
	Offset uint64

	// Parcel contains the event from the stream.
	Parcel parcel.Parcel
}

// ID returns the ID of the message.
func (e Event) ID() string {
	return e.Parcel.ID()
}
