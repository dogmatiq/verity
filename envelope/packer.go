package envelope

import (
	"fmt"
	"strings"
	"time"

	"github.com/dogmatiq/configkit"
	"github.com/dogmatiq/configkit/message"
	"github.com/dogmatiq/dogma"
	"github.com/dogmatiq/infix/draftspecs/envelopespec"
	"github.com/dogmatiq/marshalkit"
	"github.com/google/uuid"
)

// Packer puts messages into envelopes.
type Packer struct {
	// Application is the identity of this application.
	Application configkit.Identity

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

// PackCommand returns a new command envelope containing the given message.
func (p *Packer) PackCommand(m dogma.Message) *Envelope {
	p.checkProducedRole(m, message.CommandRole)
	return p.new(m)
}

// PackEvent returns a new event envelope containing the given message.
func (p *Packer) PackEvent(m dogma.Message) *Envelope {
	p.checkProducedRole(m, message.EventRole)
	return p.new(m)
}

// PackChildCommand returns a new command envelope containing the given message
// and configured as a child of cause.
func (p *Packer) PackChildCommand(
	parent *envelopespec.Envelope,
	cause, m dogma.Message,
	handler configkit.Identity,
	instanceID string,
) *Envelope {
	p.checkConsumedRole(cause, message.EventRole, message.TimeoutRole)
	p.checkProducedRole(m, message.CommandRole)

	return p.newChild(
		parent,
		m,
		handler,
		instanceID,
	)
}

// PackChildEvent returns a new event envelope containing the given message and
// configured as a child of cause.
func (p *Packer) PackChildEvent(
	parent *envelopespec.Envelope,
	cause, m dogma.Message,
	handler configkit.Identity,
	instanceID string,
) *Envelope {
	p.checkConsumedRole(cause, message.CommandRole)
	p.checkProducedRole(m, message.EventRole)

	return p.newChild(
		parent,
		m,
		handler,
		instanceID,
	)
}

// PackChildTimeout returns a new timeout envelope containing the given message
// and configured as a child of cause.
func (p *Packer) PackChildTimeout(
	parent *envelopespec.Envelope,
	cause, m dogma.Message,
	t time.Time,
	handler configkit.Identity,
	instanceID string,
) *Envelope {
	p.checkConsumedRole(cause, message.EventRole, message.TimeoutRole)
	p.checkProducedRole(m, message.TimeoutRole)

	env := p.newChild(
		parent,
		m,
		handler,
		instanceID,
	)

	env.ScheduledFor = t

	return env
}

// new returns an envelope containing the given message.
func (p *Packer) new(m dogma.Message) *Envelope {
	id := p.generateID()

	return &Envelope{
		MetaData{
			id,
			id,
			id,
			Source{
				Application: p.Application,
			},
			p.now(),
			time.Time{},
		},
		m,
	}
}

// newChild returns an envelope containing the given message, which was a
// produced as a result of handling a causal message.
func (p *Packer) newChild(
	parent *envelopespec.Envelope,
	m dogma.Message,
	handler configkit.Identity,
	instanceID string,
) *Envelope {
	env := p.new(m)

	env.CausationID = parent.MetaData.MessageId
	env.CorrelationID = parent.MetaData.CorrelationId
	env.Source.Handler = handler
	env.Source.InstanceID = instanceID

	return env
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

	return uuid.New().String()
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
