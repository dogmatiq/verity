package eventstore

import (
	"github.com/dogmatiq/infix/envelope"
	"github.com/dogmatiq/marshalkit"
)

// Offset is the position of an event within the store.
type Offset uint64

// Event is an event persisted in the store.
type Event struct {
	Offset   Offset
	MetaData envelope.MetaData
	Packet   marshalkit.Packet
}
