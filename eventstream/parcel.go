package eventstream

import "github.com/dogmatiq/infix/envelope"

// Offset is the 0-based index of an event on a stream.
type Offset uint64

// Parcel is a container for an envelope and event stream specific meta-data.
type Parcel struct {
	// Offset is the offset of the message on the stream.
	Offset Offset

	// Envelope contains the event from the stream.
	Envelope *envelope.Envelope
}

// ID returns the ID of the message in the parcel.
func (p Parcel) ID() string {
	return p.Envelope.MetaData.MessageID
}
