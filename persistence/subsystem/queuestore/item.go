package queuestore

import (
	"time"

	"github.com/dogmatiq/infix/draftspecs/envelopespec"
)

// Item is a message persisted in the queue store.
type Item struct {
	Revision      uint64
	FailureCount  uint
	NextAttemptAt time.Time
	Envelope      *envelopespec.Envelope
}

// ID returns the ID of the message.
func (i *Item) ID() string {
	return i.Envelope.MetaData.MessageId
}
