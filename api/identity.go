package api

import (
	"github.com/dogmatiq/configkit"
	"github.com/dogmatiq/infix/api/internal/pb"
)

// marshalIdentity marshals an identity to its protobuf representation.
func marshalIdentity(src configkit.Identity) *pb.Identity {
	return &pb.Identity{
		Name: src.Name,
		Key:  src.Key,
	}
}

// unmarshalIdentity unmarshals an identity from its protobuf representation.
func unmarshalIdentity(src *pb.Identity, dest *configkit.Identity) error {
	dest.Name = src.GetName()
	dest.Key = src.GetKey()

	return dest.Validate()
}
