package envelope

import (
	"github.com/dogmatiq/dogma"
)

// Envelope is a container for a message and its meta-data.
type Envelope struct {
	MetaData

	// Message is the in-memory representation of the message, as used by the
	// application.
	Message dogma.Message
}
