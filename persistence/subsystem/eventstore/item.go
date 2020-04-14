package eventstore

import (
	"github.com/dogmatiq/infix/draftspecs/envelopespec"
)

// Offset is the position of an event within the store.
type Offset uint64

// Item is a message persisted in the event store.
type Item struct {
	Offset   Offset
	Envelope *envelopespec.Envelope
}

// ID returns the ID of the message.
func (i *Item) ID() string {
	return i.Envelope.MetaData.MessageId
}
