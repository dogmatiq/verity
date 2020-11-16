package queue

import (
	"github.com/dogmatiq/verity/parcel"
	"github.com/dogmatiq/verity/persistence"
)

// Message is a message on the queue.
type Message struct {
	persistence.QueueMessage
	Parcel parcel.Parcel
}
