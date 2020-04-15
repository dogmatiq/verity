package queuestore

import (
	"time"

	"github.com/dogmatiq/infix/draftspecs/envelopespec"
)

// Revision is the revision of a message on the queue, used for optimistic
// concurrency control.
type Revision uint64

// Item is a message persisted in the queue store.
type Item struct {
	Revision      Revision
	FailureCount  uint
	NextAttemptAt time.Time
	Envelope      *envelopespec.Envelope
}

// ID returns the ID of the message.
func (i *Item) ID() string {
	return i.Envelope.MetaData.MessageId
}
