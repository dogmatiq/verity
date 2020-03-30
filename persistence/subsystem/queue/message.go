package queue

import (
	"time"

	"github.com/dogmatiq/infix/draftspecs/envelopespec"
)

// Revision is the revision of a message on the queue, used for optimistic
// concurrency control.
type Revision uint64

// Message is a message persisted on the queue.
type Message struct {
	Revision      Revision
	NextAttemptAt time.Time
	Envelope      *envelopespec.Envelope
}
