package api

import (
	"github.com/dogmatiq/infix/api/internal/pb"
	"github.com/dogmatiq/marshalkit"
)

// marshalPacket marshals a packet to its protobuf representation.
func marshalPacket(src marshalkit.Packet) *pb.Packet {
	return &pb.Packet{
		MediaType: src.MediaType,
		Data:      src.Data,
	}
}

// unmarshalPacket unmarshals a packet from its protobuf representation.
func unmarshalPacket(src *pb.Packet, dest *marshalkit.Packet) {
	dest.MediaType = src.MediaType
	dest.Data = src.GetData()
}
