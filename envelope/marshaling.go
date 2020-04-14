package envelope

import (
	"time"

	"github.com/dogmatiq/configkit"
	"github.com/dogmatiq/dogma"
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
		MetaData:     marshalMetaData(env.Message, &env.MetaData),
		PortableName: n,
		MediaType:    p.MediaType,
		Data:         p.Data,
	}, nil
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

	out.Message, err = UnmarshalMessage(m, env)

	return &out, err
}

// MustMarshalMessage marshals a Dogma message into an envelope or panics if
// unable to do so.
func MustMarshalMessage(
	vm marshalkit.ValueMarshaler,
	m dogma.Message,
	env *envelopespec.Envelope,
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

// UnmarshalMessage unmarshals a message from an envelope.
func UnmarshalMessage(
	m marshalkit.ValueMarshaler,
	env *envelopespec.Envelope,
) (dogma.Message, error) {
	return marshalkit.UnmarshalMessage(
		m,
		marshalkit.Packet{
			MediaType: env.GetMediaType(),
			Data:      env.GetData(),
		},
	)
}

// marshalMetaData marshals message meta-data to its protobuf representation.
func marshalMetaData(m dogma.Message, in *MetaData) *envelopespec.MetaData {
	return &envelopespec.MetaData{
		MessageId:     in.MessageID,
		CausationId:   in.CausationID,
		CorrelationId: in.CorrelationID,
		Source:        marshalSource(&in.Source),
		CreatedAt:     MarshalTime(in.CreatedAt),
		ScheduledFor:  MarshalTime(in.ScheduledFor),
		Description:   dogma.DescribeMessage(m),
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

	var err error

	out.CreatedAt, err = UnmarshalTime(in.GetCreatedAt())
	if err != nil {
		return err
	}

	out.ScheduledFor, err = UnmarshalTime(in.GetScheduledFor())
	if err != nil {
		return err
	}

	return out.Validate()
}

// marshalSource marshals a message source to its protobuf representation.
func marshalSource(in *Source) *envelopespec.Source {
	out := &envelopespec.Source{
		Application: MarshalIdentity(in.Application),
	}

	if !in.Handler.IsZero() {
		out.Handler = MarshalIdentity(in.Handler)
		out.InstanceId = in.InstanceID
	}

	return out
}

// unmarshalSource unmarshals a message source from its protobuf representation.
func unmarshalSource(in *envelopespec.Source, out *Source) error {
	UnmarshalIdentity(in.GetApplication(), &out.Application)
	UnmarshalIdentity(in.GetHandler(), &out.Handler)
	out.InstanceID = in.GetInstanceId()

	return out.Validate()
}

// MarshalIdentity marshals a configkit.Identity to its protocol buffers
// representation.
func MarshalIdentity(in configkit.Identity) *envelopespec.Identity {
	return &envelopespec.Identity{
		Name: in.Name,
		Key:  in.Key,
	}
}

// UnmarshalIdentity unmarshals a configkit.Identity from its protocol buffers
// representation.
func UnmarshalIdentity(
	in *envelopespec.Identity,
	out *configkit.Identity,
) (err error) {
	out.Name = in.GetName()
	out.Key = in.GetKey()

	return out.Validate()
}

// MarshalTime marshals a time.Time to its RFC-3339 representation.
func MarshalTime(in time.Time) string {
	if in.IsZero() {
		return ""
	}

	return in.Format(time.RFC3339Nano)
}

// UnmarshalTime unmarshals a time.Time from its RFC-3339 representation.
func UnmarshalTime(in string) (time.Time, error) {
	if len(in) == 0 {
		return time.Time{}, nil
	}

	return time.Parse(time.RFC3339Nano, in)
}
