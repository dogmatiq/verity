package parcel

import (
	"time"

	"github.com/dogmatiq/dogma"
	"github.com/dogmatiq/interopspec/envelopespec"
	"github.com/dogmatiq/marshalkit"
)

// A Parcel is a container for an envelope and the original information that was
// used to create it.
type Parcel struct {
	// Envelope is the message envelope.
	Envelope *envelopespec.Envelope

	// Message is the original representation of the message.
	Message dogma.Message

	// CreatedAt is the time at which the message was created.
	CreatedAt time.Time

	// ScheduledFor is the time at which a timeout message is scheduled to
	// occur. If the message is not a timeout message it is the zero-value.
	ScheduledFor time.Time
}

// ID returns the ID of the message.
func (p Parcel) ID() string {
	return p.Envelope.GetMessageId()
}

// FromEnvelope constructs a parcel from an envelope.
func FromEnvelope(
	ma marshalkit.ValueMarshaler,
	env *envelopespec.Envelope,
) (Parcel, error) {
	if err := env.Validate(); err != nil {
		return Parcel{}, err
	}

	m, err := marshalkit.UnmarshalMessageFromEnvelope(ma, env)
	if err != nil {
		return Parcel{}, err
	}

	createdAt, err := marshalkit.UnmarshalEnvelopeTime(env.GetCreatedAt())
	if err != nil {
		return Parcel{}, err
	}

	scheduledFor, err := marshalkit.UnmarshalEnvelopeTime(env.GetScheduledFor())
	if err != nil {
		return Parcel{}, err
	}

	return Parcel{
		Envelope:     env,
		Message:      m,
		CreatedAt:    createdAt,
		ScheduledFor: scheduledFor,
	}, nil
}
