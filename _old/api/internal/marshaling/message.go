package marshaling

import (
	"github.com/dogmatiq/infix/api/internal/pb"
	"github.com/dogmatiq/infix/envelope"
	"github.com/dogmatiq/marshalkit"
)

// MarshalMessageEnvelope marshals a message envelope to its protobuf
// representation.
func MarshalMessageEnvelope(src *envelope.Envelope) *pb.MessageEnvelope {
	return &pb.MessageEnvelope{
		MetaData: MarshalMessageMetaData(&src.MetaData),
		Packet:   MarshalPacket(src.Packet),
	}
}

// UnmarshalMessageEnvelope unmarshals a message envelope from its protobuf
// representation.
func UnmarshalMessageEnvelope(
	ma marshalkit.Marshaler,
	src *pb.MessageEnvelope,
	dest *envelope.Envelope,
) error {
	err := UnmarshalMessageMetaData(src.GetMetaData(), &dest.MetaData)
	if err != nil {
		return err
	}

	UnmarshalPacket(src.Packet, &dest.Packet)
	dest.Message, err = marshalkit.UnmarshalMessage(ma, dest.Packet)

	return err
}

// MarshalMessageMetaData marshals message meta-data to its protobuf
// representation.
func MarshalMessageMetaData(src *envelope.MetaData) *pb.MessageMetaData {
	dest := &pb.MessageMetaData{
		MessageId:     src.MessageID,
		CausationId:   src.CausationID,
		CorrelationId: src.CorrelationID,
		Source:        MarshalMessageSource(&src.Source),
		CreatedAt:     MarshalTime(src.CreatedAt),
		ScheduledFor:  MarshalTime(src.ScheduledFor),
	}

	return dest
}

// UnmarshalMessageMetaData unmarshals message data from its protobuf
// representation.
func UnmarshalMessageMetaData(src *pb.MessageMetaData, dest *envelope.MetaData) error {
	dest.MessageID = src.GetMessageId()
	dest.CausationID = src.GetCausationId()
	dest.CorrelationID = src.GetCorrelationId()

	if err := UnmarshalMessageSource(src.GetSource(), &dest.Source); err != nil {
		return err
	}

	if err := UnmarshalTime(src.GetCreatedAt(), &dest.CreatedAt); err != nil {
		return err
	}

	if err := UnmarshalTime(src.GetScheduledFor(), &dest.ScheduledFor); err != nil {
		return err
	}

	return dest.Validate()
}

// MarshalMessageSource marshals a message source to its protobuf
// representation.
func MarshalMessageSource(src *envelope.Source) *pb.MessageSource {
	dest := &pb.MessageSource{
		Application: MarshalIdentity(src.Application),
		InstanceId:  src.InstanceID,
	}

	if !src.Handler.IsZero() {
		dest.Handler = MarshalIdentity(src.Handler)
	}

	return dest
}

// UnmarshalMessageSource marshals a message source to its protobuf
// representation.
func UnmarshalMessageSource(src *pb.MessageSource, dest *envelope.Source) error {
	dest.InstanceID = src.GetInstanceId()

	if err := UnmarshalIdentity(src.GetApplication(), &dest.Application); err != nil {
		return err
	}

	if err := UnmarshalIdentity(src.GetHandler(), &dest.Handler); err != nil {
		if !dest.Handler.IsZero() {
			return err
		}
	}

	return dest.Validate()
}
