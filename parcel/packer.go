package parcel

import (
	"fmt"
	"strings"
	"time"

	"github.com/dogmatiq/configkit/message"
	"github.com/dogmatiq/dogma"
	"github.com/dogmatiq/interopspec/envelopespec"
	"github.com/dogmatiq/marshalkit"
	"github.com/google/uuid"
)

// Packer puts messages into parcels.
type Packer struct {
	// Application is the identity of this application.
	Application *envelopespec.Identity

	// Marshaler is used to marshal messages into envelopes.
	Marshaler marshalkit.ValueMarshaler

	// Produced is a map of message type to role, used to validate the messages
	// that are being packed.
	Produced message.TypeRoles

	// Consumed is a map of message type to role for consumed messages, which
	// can be the cause of new messages.
	Consumed message.TypeRoles

	// GenerateID is a function used to generate new message IDs. If it is nil,
	// a UUID is generated.
	GenerateID func() string

	// Now is a function used to get the current time. If it is nil, time.Now()
	// is used.
	Now func() time.Time
}

// PackCommand returns a parcel containing the given command message.
func (p *Packer) PackCommand(m dogma.Message) Parcel {
	p.checkProducedRole(m, message.CommandRole)
	return p.new(m)
}

// PackEvent returns a parcel containing the given event message.
func (p *Packer) PackEvent(m dogma.Message) Parcel {
	p.checkProducedRole(m, message.EventRole)
	return p.new(m)
}

// PackChildCommand returns a parcel containing the given command message,
// configured as a child of c, the cause.
func (p *Packer) PackChildCommand(
	c Parcel,
	m dogma.Message,
	handler *envelopespec.Identity,
	instanceID string,
) Parcel {
	p.checkConsumedRole(c.Message, message.EventRole, message.TimeoutRole)
	p.checkProducedRole(m, message.CommandRole)

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
	m dogma.Message,
	handler *envelopespec.Identity,
	instanceID string,
) Parcel {
	p.checkConsumedRole(c.Message, message.CommandRole)
	p.checkProducedRole(m, message.EventRole)

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
	m dogma.Message,
	t time.Time,
	handler *envelopespec.Identity,
	instanceID string,
) Parcel {
	p.checkConsumedRole(c.Message, message.EventRole, message.TimeoutRole)
	p.checkProducedRole(m, message.TimeoutRole)

	parcel := p.newChild(
		c,
		m,
		handler,
		instanceID,
	)

	parcel.Envelope.ScheduledFor = marshalkit.MustMarshalEnvelopeTime(t)
	parcel.ScheduledFor = t

	return parcel
}

// new returns an envelope containing the given message.
func (p *Packer) new(m dogma.Message) Parcel {
	id := p.generateID()
	now := p.now()

	env := Parcel{
		Envelope: &envelopespec.Envelope{
			MessageId:         id,
			CorrelationId:     id,
			CausationId:       id,
			SourceApplication: p.Application,
			CreatedAt:         marshalkit.MustMarshalEnvelopeTime(now),
			Description:       dogma.DescribeMessage(m),
		},
		Message:   m,
		CreatedAt: now,
	}

	marshalkit.MustMarshalMessageIntoEnvelope(p.Marshaler, m, env.Envelope)

	return env
}

// newChild returns an envelope containing the given message, which was a
// produced as a result of handling a causal message.
func (p *Packer) newChild(
	c Parcel,
	m dogma.Message,
	handler *envelopespec.Identity,
	instanceID string,
) Parcel {
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

// checkProducedRole panics if mt does not fill one of the the given roles.
func (p *Packer) checkProducedRole(m dogma.Message, roles ...message.Role) {
	mt := message.TypeOf(m)
	x, ok := p.Produced[mt]

	if !ok {
		panic(fmt.Sprintf("%s is not a recognised message type", mt))
	}

	mustBeOneOf(mt, x, roles)
}

// checkConsumedRole panics if mt does not fill one of the the given roles.
func (p *Packer) checkConsumedRole(m dogma.Message, roles ...message.Role) {
	mt := message.TypeOf(m)
	x, ok := p.Consumed[mt]

	if !ok {
		panic(fmt.Sprintf("%s is not consumed by this handler", mt))
	}

	mustBeOneOf(mt, x, roles)
}

// mustBe panics if x is not in roles.
func mustBeOneOf(mt message.Type, x message.Role, roles []message.Role) {
	if x.Is(roles...) {
		return
	}

	var valid []string
	for _, r := range roles {
		valid = append(valid, r.String())
	}

	message := fmt.Sprintf(
		"%s is a %s, expected a %s",
		mt,
		x,
		strings.Join(valid, " or "),
	)
	message = strings.ReplaceAll(message, "a event", "an event")
	panic(message)
}
