package api

import (
	"time"

	"github.com/dogmatiq/configkit"
	"github.com/dogmatiq/infix/api/internal/pb"
	"github.com/dogmatiq/infix/envelope"
	"github.com/dogmatiq/marshalkit"
)

// marshalIdentity marshals a configkit.Identity to its protocol buffers
// representation.
func marshalIdentity(in configkit.Identity) *pb.Identity {
	return &pb.Identity{
		Name: in.Name,
		Key:  in.Key,
	}
}

// unmarshalIdentity unmarshals a configkit.Identity from its protocol buffers
// representation.
func unmarshalIdentity(in *pb.Identity, out *configkit.Identity) (err error) {
	out.Name = in.GetName()
	out.Key = in.GetKey()

	return out.Validate()
}

// marshalEnvelope marshals a message envelope to its protobuf representation.
func marshalEnvelope(in *envelope.Envelope) *pb.Envelope {
	return &pb.Envelope{
		MetaData: marshalMetaData(&in.MetaData),
		Packet:   marshalPacket(in.Packet),
	}
}

// unmarshalEnvelope unmarshals a message envelope from its protobuf
// representation.
func unmarshalEnvelope(
	m marshalkit.Marshaler,
	in *pb.Envelope,
	out *envelope.Envelope,
) error {
	err := unmarshalMetaData(in.GetMetaData(), &out.MetaData)
	if err != nil {
		return err
	}

	unmarshalPacket(in.Packet, &out.Packet)
	out.Message, err = marshalkit.UnmarshalMessage(m, out.Packet)

	return err
}

// marshalMetaData marshals message meta-data to its protobuf representation.
func marshalMetaData(in *envelope.MetaData) *pb.MetaData {
	return &pb.MetaData{
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
func unmarshalMetaData(in *pb.MetaData, out *envelope.MetaData) error {
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
func marshalSource(in *envelope.Source) *pb.Source {
	out := &pb.Source{
		Application: marshalIdentity(in.Application),
	}

	if !in.Handler.IsZero() {
		out.Handler = marshalIdentity(in.Handler)
		out.InstanceId = in.InstanceID
	}

	return out
}

// unmarshalSource unmarshals a message source from its protobuf representation.
func unmarshalSource(in *pb.Source, out *envelope.Source) error {
	unmarshalIdentity(in.GetApplication(), &out.Application)
	unmarshalIdentity(in.GetHandler(), &out.Handler)
	out.InstanceID = in.GetInstanceId()

	return out.Validate()
}

// marshalPacket marshals a packet to its protobuf representation.
func marshalPacket(in marshalkit.Packet) *pb.Packet {
	return &pb.Packet{
		MediaType: in.MediaType,
		Data:      in.Data,
	}
}

// unmarshalPacket unmarshals a packet from its protobuf representation.
func unmarshalPacket(in *pb.Packet, out *marshalkit.Packet) {
	out.MediaType = in.GetMediaType()
	out.Data = in.GetData()
}

// marshalTime marshals a time.Time to its protobuf representation.
func marshalTime(in time.Time) []byte {
	if in.IsZero() {
		return nil
	}

	buf, err := in.MarshalBinary()
	if err != nil {
		panic(err)
	}

	return buf
}

// unmarshalTime unmarshals a time.Time from its protobuf representation.
func unmarshalTime(in []byte, out *time.Time) error {
	if len(in) == 0 {
		*out = time.Time{}
		return nil
	}

	return out.UnmarshalBinary(in)
}
