package marshaling

import (
	"github.com/dogmatiq/infix/api/internal/pb"
	"github.com/dogmatiq/marshalkit"
)

// MarshalPacket marshals a packet to its protobuf representation.
func MarshalPacket(src marshalkit.Packet) *pb.Packet {
	return &pb.Packet{
		MediaType: src.MediaType,
		Data:      src.Data,
	}
}

// UnmarshalPacket unmarshals a packet from its protobuf representation.
func UnmarshalPacket(src *pb.Packet, dest *marshalkit.Packet) {
	dest.MediaType = src.MediaType
	dest.Data = src.GetData()
}
