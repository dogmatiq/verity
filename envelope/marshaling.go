package envelope

import (
	"time"

	"github.com/dogmatiq/configkit"
	"github.com/dogmatiq/infix/draftspecs/envelopespec"
	"github.com/dogmatiq/marshalkit"
)

// Marshal marshals a message envelope to its protobuf representation.
func Marshal(
	m marshalkit.ValueMarshaler,
	env *Envelope,
) (*envelopespec.Envelope, error) {
	p, err := marshalkit.MarshalMessage(m, env.Message)
	if err != nil {
		return nil, err
	}

	_, n, err := p.ParseMediaType()
	if err != nil {
		// CODE COVERAGE: This branch would require the marshaler to violate its
		// own requirements on the format of the media-type.
		panic(err)
	}

	return &envelopespec.Envelope{
		MetaData:     marshalMetaData(&env.MetaData),
		PortableName: n,
		MediaType:    p.MediaType,
		Data:         p.Data,
	}, nil
}

// MarshalMany marshals multiple message envelopes to their protobuf
// representations.
func MarshalMany(
	m marshalkit.ValueMarshaler,
	envelopes []*Envelope,
) ([]*envelopespec.Envelope, error) {
	out := make([]*envelopespec.Envelope, len(envelopes))

	var err error
	for i, env := range envelopes {
		out[i], err = Marshal(m, env)
		if err != nil {
			return nil, err
		}
	}

	return out, nil
}

// MustMarshal marshals a message envelope to its protobuf representation, or
// panics if it is unable to do so.
func MustMarshal(
	m marshalkit.ValueMarshaler,
	in *Envelope,
) *envelopespec.Envelope {
	out, err := Marshal(m, in)
	if err != nil {
		panic(err)
	}

	return out
}

// MustMarshalMany marshals multiple messages envelope to their protobuf
// representations, or panics if it is unable to do so.
func MustMarshalMany(
	m marshalkit.ValueMarshaler,
	envelopes []*Envelope,
) []*envelopespec.Envelope {
	out, err := MarshalMany(m, envelopes)
	if err != nil {
		panic(err)
	}

	return out
}

// Unmarshal unmarshals a message envelope from its protobuf representation.
func Unmarshal(
	m marshalkit.ValueMarshaler,
	env *envelopespec.Envelope,
) (*Envelope, error) {
	var out Envelope

	err := unmarshalMetaData(env.GetMetaData(), &out.MetaData)
	if err != nil {
		return nil, err
	}

	out.Message, err = marshalkit.UnmarshalMessage(
		m,
		marshalkit.Packet{
			MediaType: env.GetMediaType(),
			Data:      env.GetData(),
		},
	)

	return &out, err
}

// marshalMetaData marshals message meta-data to its protobuf representation.
func marshalMetaData(in *MetaData) *envelopespec.MetaData {
	return &envelopespec.MetaData{
		MessageId:     in.MessageID,
		CausationId:   in.CausationID,
		CorrelationId: in.CorrelationID,
		Source:        marshalSource(&in.Source),
		CreatedAt:     marshalTime(in.CreatedAt),
		ScheduledFor:  marshalTime(in.ScheduledFor),
	}
}

// unmarshalMetaData unmarshals message meta-data from its protobuf
// representation.
func unmarshalMetaData(in *envelopespec.MetaData, out *MetaData) error {
	out.MessageID = in.GetMessageId()
	out.CausationID = in.GetCausationId()
	out.CorrelationID = in.GetCorrelationId()

	if err := unmarshalSource(in.GetSource(), &out.Source); err != nil {
		return err
	}

	if err := unmarshalTime(in.GetCreatedAt(), &out.CreatedAt); err != nil {
		return err
	}

	if err := unmarshalTime(in.GetScheduledFor(), &out.ScheduledFor); err != nil {
		return err
	}

	return out.Validate()
}

// marshalSource marshals a message source to its protobuf representation.
func marshalSource(in *Source) *envelopespec.Source {
	out := &envelopespec.Source{
		Application: marshalIdentity(in.Application),
	}

	if !in.Handler.IsZero() {
		out.Handler = marshalIdentity(in.Handler)
		out.InstanceId = in.InstanceID
	}

	return out
}

// unmarshalSource unmarshals a message source from its protobuf representation.
func unmarshalSource(in *envelopespec.Source, out *Source) error {
	unmarshalIdentity(in.GetApplication(), &out.Application)
	unmarshalIdentity(in.GetHandler(), &out.Handler)
	out.InstanceID = in.GetInstanceId()

	return out.Validate()
}

// marshalIdentity marshals a configkit.Identity to its protocol buffers
// representation.
func marshalIdentity(in configkit.Identity) *envelopespec.Identity {
	return &envelopespec.Identity{
		Name: in.Name,
		Key:  in.Key,
	}
}

// unmarshalIdentity unmarshals a configkit.Identity from its protocol buffers
// representation.
func unmarshalIdentity(
	in *envelopespec.Identity,
	out *configkit.Identity,
) (err error) {
	out.Name = in.GetName()
	out.Key = in.GetKey()

	return out.Validate()
}

// marshalTime marshals a time.Time to its RFC-3339 representation.
func marshalTime(in time.Time) string {
	if in.IsZero() {
		return ""
	}

	return in.Format(time.RFC3339Nano)
}

// unmarshalTime unmarshals a time.Time from its RFC-3339 representation.
func unmarshalTime(in string, out *time.Time) error {
	if len(in) == 0 {
		*out = time.Time{}
		return nil
	}

	var err error
	*out, err = time.Parse(time.RFC3339Nano, in)
	return err
}
