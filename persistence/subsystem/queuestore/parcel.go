package queuestore

import (
	"time"

	"github.com/dogmatiq/dogma"
	"github.com/dogmatiq/infix/draftspecs/envelopespec"
)

// Revision is the revision of a message on the queue, used for optimistic
// concurrency control.
type Revision uint64

// Parcel is a container for an envelope and queue store specific meta-data.
type Parcel struct {
	Revision      Revision
	FailureCount  uint
	NextAttemptAt time.Time
	Envelope      *envelopespec.Envelope
}

// ID returns the ID of the message in the parcel.
func (p *Parcel) ID() string {
	return p.Envelope.MetaData.MessageId
}

// Pair encapsulates a parcel and the envelope that is encoded within it.
type Pair struct {
	Parcel  *Parcel
	Message dogma.Message
}
