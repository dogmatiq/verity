package eventstore

import (
	"github.com/dogmatiq/infix/draftspecs/envelopespec"
)

// Item is a message persisted in the event store.
type Item struct {
	Offset   uint64
	Envelope *envelopespec.Envelope
}

// ID returns the ID of the message.
func (i *Item) ID() string {
	return i.Envelope.MetaData.MessageId
}
