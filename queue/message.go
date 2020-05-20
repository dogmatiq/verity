package queue

import (
	"github.com/dogmatiq/infix/parcel"
	"github.com/dogmatiq/infix/persistence/subsystem/queuestore"
)

// Message is a message on the queue.
type Message struct {
	Parcel *parcel.Parcel
	Item   *queuestore.Item
}

// ID returns the message ID.
func (m Message) ID() string {
	return m.Item.ID()
}
