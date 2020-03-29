package queue

import "github.com/dogmatiq/infix/draftspecs/envelopespec"

// Revision is the revision of a queued message, used for optimistic concurrency
// control.
type Revision uint64

// Message is a message persisted on the queue.
type Message struct {
	Revision Revision
	Envelope *envelopespec.Envelope
}
