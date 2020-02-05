package envelope

import (
	"github.com/dogmatiq/dogma"
	"github.com/dogmatiq/marshalkit"
)

// Envelope is a container for a message and its meta-data.
type Envelope struct {
	MetaData

	// Message is the in-memory representation of the message, as used by the
	// application.
	Message dogma.Message

	// Packet is the serialized representation of the message.
	Packet marshalkit.Packet
}
