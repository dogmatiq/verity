package envelope

import (
	"time"

	"github.com/dogmatiq/configkit"
	"github.com/dogmatiq/infix/draftspecs/envelopespec"
	"github.com/dogmatiq/marshalkit"
)

// Marshal marshals a message envelope to its protobuf representation.
func Marshal(in *Envelope) (*envelopespec.Envelope, error) {
	md, err := marshalMetaData(&in.MetaData)
	if err != nil {
		return nil, err
	}

	return &envelopespec.Envelope{
		MetaData:  md,
		MediaType: in.Packet.MediaType,
		Data:      in.Packet.Data,
	}, nil
}

// MustMarshal marshals a message envelope to its protobuf representation, or
// panics if it is unable to do so.
func MustMarshal(in *Envelope) *envelopespec.Envelope {
	out, err := Marshal(in)
	if err != nil {
		panic(err)
	}

	return out
}

// Unmarshal unmarshals a message envelope from its protobuf representation.
func Unmarshal(
	m marshalkit.ValueMarshaler,
	in *envelopespec.Envelope,
	out *Envelope,
) error {
	err := unmarshalMetaData(in.GetMetaData(), &out.MetaData)
	if err != nil {
		return err
	}

	out.Packet.MediaType = in.GetMediaType()
	out.Packet.Data = in.GetData()

	out.Message, err = marshalkit.UnmarshalMessage(m, out.Packet)

	return err
}

// marshalMetaData marshals message meta-data to its protobuf representation.
func marshalMetaData(in *MetaData) (*envelopespec.MetaData, error) {
	createdAt, err := marshalTime(in.CreatedAt)
	if err != nil {
		return nil, err
	}

	scheduledFor, err := marshalTime(in.ScheduledFor)
	if err != nil {
		return nil, err
	}

	return &envelopespec.MetaData{
		MessageId:     in.MessageID,
		CausationId:   in.CausationID,
		CorrelationId: in.CorrelationID,
		Source:        marshalSource(&in.Source),
		CreatedAt:     createdAt,
		ScheduledFor:  scheduledFor,
	}, nil
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

// marshalTime marshals a time.Time to its protocol buffers representation.
func marshalTime(in time.Time) ([]byte, error) {
	if in.IsZero() {
		return nil, nil
	}

	return in.MarshalBinary()
}

// unmarshalTime unmarshals a time.Time from its protocol buffers
// representation.
func unmarshalTime(in []byte, out *time.Time) error {
	if len(in) == 0 {
		*out = time.Time{}
		return nil
	}

	return out.UnmarshalBinary(in)
}
