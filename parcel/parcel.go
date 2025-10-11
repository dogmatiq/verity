package parcel

import (
	"time"

	"github.com/dogmatiq/dogma"
	"github.com/dogmatiq/enginekit/protobuf/envelopepb"
)

// A Parcel is a container for an envelope and the original information that was
// used to create it.
type Parcel struct {
	// Envelope is the message envelope.
	Envelope *envelopepb.Envelope

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
	return p.Envelope.GetMessageId().AsString()
}

// FromEnvelope constructs a parcel from an envelope.
func FromEnvelope(
	env *envelopepb.Envelope,
) (Parcel, error) {
	if err := env.Validate(); err != nil {
		return Parcel{}, err
	}

	m, err := envelopepb.Unpack(env)
	if err != nil {
		return Parcel{}, err
	}

	p := Parcel{
		Envelope: env,
		Message:  m,
	}

	p.CreatedAt = env.GetCreatedAt().AsTime()

	if t := env.GetScheduledFor(); t != nil {
		p.ScheduledFor = t.AsTime()
	}

	return p, nil
}
