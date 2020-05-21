package queue

import (
	"github.com/dogmatiq/infix/parcel"
	"github.com/dogmatiq/infix/persistence"
)

// Message is a message on the queue.
type Message struct {
	persistence.QueueMessage
	Parcel parcel.Parcel
}
