package message

import "github.com/dogmatiq/marshalkit"

// Packet is a container for a marshaled message and its meta-data.
// this application.
type Packet struct {
	MetaData
	Message marshalkit.Packet
}
