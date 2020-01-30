package marshaling

import (
	"github.com/dogmatiq/configkit"
	"github.com/dogmatiq/infix/api/internal/pb"
)

// MarshalIdentity marshals an identity to its protobuf representation.
func MarshalIdentity(src configkit.Identity) *pb.Identity {
	return &pb.Identity{
		Name: src.Name,
		Key:  src.Key,
	}
}

// UnmarshalIdentity unmarshals an identity from its protobuf representation.
func UnmarshalIdentity(src *pb.Identity, dest *configkit.Identity) error {
	dest.Name = src.GetName()
	dest.Key = src.GetKey()

	return dest.Validate()
}
