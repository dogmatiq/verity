package envelopespec

import (
	"time"

	"github.com/dogmatiq/configkit"
	"github.com/dogmatiq/dogma"
	"github.com/dogmatiq/marshalkit"
)

// MarshalMessage marshals a Dogma message into an envelope.
func MarshalMessage(
	vm marshalkit.ValueMarshaler,
	m dogma.Message,
	env *Envelope,
) {
	p := marshalkit.MustMarshalMessage(vm, m)

	_, n, err := p.ParseMediaType()
	if err != nil {
		// CODE COVERAGE: This branch would require the marshaler to violate its
		// own requirements on the format of the media-type.
		panic(err)
	}

	env.PortableName = n
	env.MediaType = p.MediaType
	env.Data = p.Data
}

// UnmarshalMessage unmarshals a Dogma message from an envelope.
func UnmarshalMessage(
	vm marshalkit.ValueMarshaler,
	env *Envelope,
) (dogma.Message, error) {
	return marshalkit.UnmarshalMessage(
		vm,
		marshalkit.Packet{
			MediaType: env.GetMediaType(),
			Data:      env.GetData(),
		},
	)
}

// MarshalIdentity marshals id to its protocol buffers representation, as used
// within message envelopes.
func MarshalIdentity(id configkit.Identity) *Identity {
	return &Identity{
		Name: id.Name,
		Key:  id.Key,
	}
}

// UnmarshalIdentity unmarshals id from its protocol buffers representation, as
// used within message envelopes.
func UnmarshalIdentity(id *Identity) (configkit.Identity, error) {
	return configkit.NewIdentity(id.Name, id.Key)
}

// MarshalTime marshals t to its RFC-3339 representation, as used within
// envelopes.
func MarshalTime(t time.Time) string {
	if t.IsZero() {
		return ""
	}

	return t.Format(time.RFC3339Nano)
}

// UnmarshalTime unmarshals a t from its RFC-3339 representation, as
// used within envelopes.
func UnmarshalTime(t string) (time.Time, error) {
	if len(t) == 0 {
		return time.Time{}, nil
	}

	return time.Parse(time.RFC3339Nano, t)
}
