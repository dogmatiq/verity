package envelope

import (
	"github.com/dogmatiq/infix/draftspecs/envelopespec"
	"github.com/dogmatiq/marshalkit"
	"github.com/golang/protobuf/proto"
)

// MarshalBinary marshals a message envelope to its binary protobuf representation.
func MarshalBinary(in *Envelope) ([]byte, error) {
	out, err := Marshal(in)
	if err != nil {
		return nil, err
	}

	return proto.Marshal(out)
}

// MustMarshalBinary marshals a message envelope to its binary protobuf representation, or
// panics if it is unable to do so.
func MustMarshalBinary(in *Envelope) []byte {
	out, err := MarshalBinary(in)
	if err != nil {
		panic(err)
	}

	return out
}

// UnmarshalBinary unmarshals a message envelope from its binary protobuf representation.
func UnmarshalBinary(
	m marshalkit.ValueMarshaler,
	data []byte,
	out *Envelope,
) error {
	var in envelopespec.Envelope

	if err := proto.Unmarshal(data, &in); err != nil {
		return err
	}

	return Unmarshal(m, &in, out)
}
