package parcel

import (
	"fmt"
	"reflect"
	"time"

	"github.com/dogmatiq/dogma"
	"github.com/dogmatiq/enginekit/marshaler"
	"github.com/dogmatiq/interopspec/envelopespec"
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
	ma marshaler.Marshaler,
	env *envelopespec.Envelope,
) (Parcel, error) {
	if err := env.Validate(); err != nil {
		return Parcel{}, err
	}

	v, err := ma.Unmarshal(marshaler.Packet{
		MediaType: env.MediaType,
		Data:      env.Data,
	})
	if err != nil {
		return Parcel{}, err
	}

	m, ok := v.(dogma.Message)
	if !ok {
		return Parcel{}, fmt.Errorf(
			"'%s' is not a message",
			reflect.TypeOf(v),
		)
	}

	p := Parcel{
		Envelope: env,
		Message:  m,
	}

	p.CreatedAt, err = time.Parse(time.RFC3339Nano, env.GetCreatedAt())
	if err != nil {
		return Parcel{}, err
	}

	if t := env.GetScheduledFor(); t != "" {
		p.ScheduledFor, err = time.Parse(time.RFC3339Nano, t)
		if err != nil {
			return Parcel{}, err
		}
	}

	return p, nil
}
