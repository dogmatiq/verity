package parcel

import (
	"time"

	"github.com/dogmatiq/dogma"
	"github.com/dogmatiq/infix/draftspecs/envelopespec"
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

// FromEnvelope constructs a parcel from an envelope.
func FromEnvelope(
	ma marshalkit.ValueMarshaler,
	env *envelopespec.Envelope,
) (*Parcel, error) {
	if err := envelopespec.CheckWellFormed(env); err != nil {
		return nil, err
	}

	m, err := envelopespec.UnmarshalMessage(ma, env)
	if err != nil {
		return nil, err
	}

	createdAt, err := envelopespec.UnmarshalTime(env.MetaData.CreatedAt)
	if err != nil {
		return nil, err
	}

	scheduledFor, err := envelopespec.UnmarshalTime(env.MetaData.ScheduledFor)
	if err != nil {
		return nil, err
	}

	return &Parcel{
		Envelope:     env,
		Message:      m,
		CreatedAt:    createdAt,
		ScheduledFor: scheduledFor,
	}, nil
}
