package api

import (
	"github.com/dogmatiq/infix/api/internal/pb"
	"github.com/dogmatiq/infix/message"
	"github.com/dogmatiq/marshalkit"
)

// marshalMessageEnvelope marshals a message envelope to its protobuf
// representation.
func marshalMessageEnvelope(src *message.Envelope) *pb.MessageEnvelope {
	return &pb.MessageEnvelope{
		MetaData: marshalMessageMetaData(&src.MetaData),
		Packet:   marshalPacket(src.Packet),
	}
}

// unmarshalMessageEnvelope unmarshals a message envelope from its protobuf
// representation.
func unmarshalMessageEnvelope(
	ma marshalkit.Marshaler,
	src *pb.MessageEnvelope,
	dest *message.Envelope,
) error {
	err := unmarshalMessageMetaData(src.GetMetaData(), &dest.MetaData)
	if err != nil {
		return err
	}

	unmarshalPacket(src.Packet, &dest.Packet)
	dest.Message, err = marshalkit.UnmarshalMessage(ma, dest.Packet)

	return err
}

// marshalMessageMetaData marshals message meta-data to its protobuf
// representation.
func marshalMessageMetaData(src *message.MetaData) *pb.MessageMetaData {
	dest := &pb.MessageMetaData{
		MessageId:     src.MessageID,
		CausationId:   src.CausationID,
		CorrelationId: src.CorrelationID,
		Source:        marshalMessageSource(&src.Source),
		CreatedAt:     marshalTime(src.CreatedAt),
		ScheduledFor:  marshalTime(src.ScheduledFor),
	}

	return dest
}

// unmarshalMessageMetaData unmarshals message data from its protobuf
// representation.
func unmarshalMessageMetaData(src *pb.MessageMetaData, dest *message.MetaData) error {
	dest.MessageID = src.GetMessageId()
	dest.CausationID = src.GetCausationId()
	dest.CorrelationID = src.GetCorrelationId()

	if err := unmarshalMessageSource(src.GetSource(), &dest.Source); err != nil {
		return err
	}

	if err := unmarshalTime(src.GetCreatedAt(), &dest.CreatedAt); err != nil {
		return err
	}

	if err := unmarshalTime(src.GetScheduledFor(), &dest.ScheduledFor); err != nil {
		return err
	}

	return dest.Validate()
}

// marshalMessageSource marshals a message source to its protobuf
// representation.
func marshalMessageSource(src *message.Source) *pb.MessageSource {
	dest := &pb.MessageSource{
		Application: marshalIdentity(src.Application),
		InstanceId:  src.InstanceID,
	}

	if !src.Handler.IsZero() {
		dest.Handler = marshalIdentity(src.Handler)
	}

	return dest
}

// unmarshalMessageSource marshals a message source to its protobuf
// representation.
func unmarshalMessageSource(src *pb.MessageSource, dest *message.Source) error {
	dest.InstanceID = src.GetInstanceId()

	if err := unmarshalIdentity(src.GetApplication(), &dest.Application); err != nil {
		return err
	}

	if err := unmarshalIdentity(src.GetHandler(), &dest.Handler); err != nil {
		if !dest.Handler.IsZero() {
			return err
		}
	}

	return dest.Validate()
}
