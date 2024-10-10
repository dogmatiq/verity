package parcel

import (
	"fmt"
	"time"

	"github.com/dogmatiq/dogma"
	"github.com/dogmatiq/enginekit/collections/sets"
	"github.com/dogmatiq/enginekit/marshaler"
	"github.com/dogmatiq/enginekit/message"
	"github.com/dogmatiq/interopspec/envelopespec"
	"github.com/google/uuid"
)

// Packer puts messages into parcels.
type Packer struct {
	// Application is the identity of this application.
	Application *envelopespec.Identity

	// Marshaler is used to marshal messages into envelopes.
	Marshaler marshaler.Marshaler

	// Produced is the set of message types that are produced by this
	// application.
	Produced *sets.Set[message.Type]

	// Consumed is the set of message types that are consumed by this
	// application.
	Consumed *sets.Set[message.Type]

	// GenerateID is a function used to generate new message IDs. If it is nil,
	// a UUID is generated.
	GenerateID func() string

	// Now is a function used to get the current time. If it is nil, time.Now()
	// is used.
	Now func() time.Time
}

// PackCommand returns a parcel containing the given command message.
func (p *Packer) PackCommand(m dogma.Command) Parcel {
	if err := m.Validate(nil); err != nil {
		panic(fmt.Sprintf("%T command is invalid: %s", m, err))
	}

	return p.new(m)
}

// PackEvent returns a parcel containing the given event message.
func (p *Packer) PackEvent(m dogma.Event) Parcel {
	if err := m.Validate(nil); err != nil {
		panic(fmt.Sprintf("%T event is invalid: %s", m, err))
	}

	return p.new(m)
}

// PackChildCommand returns a parcel containing the given command message,
// configured as a child of c, the cause.
func (p *Packer) PackChildCommand(
	c Parcel,
	m dogma.Command,
	handler *envelopespec.Identity,
	instanceID string,
) Parcel {
	if err := m.Validate(nil); err != nil {
		panic(fmt.Sprintf("%T command is invalid: %s", m, err))
	}

	return p.newChild(
		c,
		m,
		handler,
		instanceID,
	)
}

// PackChildEvent returns a parcel containing the given event message,
// configured as a child of c, the cause.
func (p *Packer) PackChildEvent(
	c Parcel,
	m dogma.Event,
	handler *envelopespec.Identity,
	instanceID string,
) Parcel {
	if err := m.Validate(nil); err != nil {
		panic(fmt.Sprintf("%T event is invalid: %s", m, err))
	}

	return p.newChild(
		c,
		m,
		handler,
		instanceID,
	)
}

// PackChildTimeout returns a parcel containing the given timeout message,
// configured as a child of c, the cause.
func (p *Packer) PackChildTimeout(
	c Parcel,
	m dogma.Timeout,
	t time.Time,
	handler *envelopespec.Identity,
	instanceID string,
) Parcel {
	if err := m.Validate(nil); err != nil {
		panic(fmt.Sprintf("%T timeout is invalid: %s", m, err))
	}

	parcel := p.newChild(
		c,
		m,
		handler,
		instanceID,
	)

	parcel.Envelope.ScheduledFor = t.Format(time.RFC3339Nano)
	parcel.ScheduledFor = t

	return parcel
}

// new returns an envelope containing the given message.
func (p *Packer) new(m dogma.Message) Parcel {
	mt := message.TypeOf(m)
	if !p.Produced.Has(mt) {
		panic(fmt.Sprintf("%s is not a recognized message type", mt))
	}

	id := p.generateID()
	now := p.now()

	packet, err := p.Marshaler.Marshal(m)
	if err != nil {
		panic(err)
	}

	portableName, err := p.Marshaler.MarshalType(mt.ReflectType())
	if err != nil {
		panic(err)
	}

	pcl := Parcel{
		Envelope: &envelopespec.Envelope{
			MessageId:         id,
			CorrelationId:     id,
			CausationId:       id,
			SourceApplication: p.Application,
			CreatedAt:         now.Format(time.RFC3339Nano),
			Description:       m.MessageDescription(),
			PortableName:      portableName,
			MediaType:         packet.MediaType,
			Data:              packet.Data,
		},
		Message:   m,
		CreatedAt: now,
	}

	return pcl
}

// newChild returns an envelope containing the given message, which was a
// produced as a result of handling a causal message.
func (p *Packer) newChild(
	c Parcel,
	m dogma.Message,
	handler *envelopespec.Identity,
	instanceID string,
) Parcel {
	ct := message.TypeOf(c.Message)
	if !p.Consumed.Has(ct) {
		panic(fmt.Sprintf("%s is not consumed by this handler", ct))
	}

	parcel := p.new(m)

	parcel.Envelope.CausationId = c.Envelope.GetMessageId()
	parcel.Envelope.CorrelationId = c.Envelope.GetCorrelationId()
	parcel.Envelope.SourceHandler = handler
	parcel.Envelope.SourceInstanceId = instanceID

	return parcel
}

// now returns the current time.
func (p *Packer) now() time.Time {
	now := p.Now
	if now == nil {
		now = time.Now
	}

	return now()
}

// generateID generates a new message ID.
func (p *Packer) generateID() string {
	if p.GenerateID != nil {
		return p.GenerateID()
	}

	return uuid.NewString()
}
