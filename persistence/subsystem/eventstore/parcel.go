package eventstore

import "github.com/dogmatiq/infix/draftspecs/envelopespec"

// Offset is the position of an event within the store.
type Offset uint64

// Parcel is a container for an envelope and event store specific meta-data.
type Parcel struct {
	Offset   Offset
	Envelope *envelopespec.Envelope
}

// ID returns the ID of the message in the parcel.
func (p *Parcel) ID() string {
	return p.Envelope.MetaData.MessageId
}