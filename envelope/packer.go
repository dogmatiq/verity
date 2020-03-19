package envelope

import (
	"time"

	"github.com/dogmatiq/configkit"
	"github.com/dogmatiq/configkit/message"
	"github.com/dogmatiq/dogma"
	"github.com/dogmatiq/marshalkit"
	"github.com/google/uuid"
)

// Packer puts messages into envelopes.
type Packer struct {
	// Application is the identity of this application.
	Application configkit.Identity

	// Marshaler is the marshaler used to marshal messages.
	Marshaler marshalkit.Marshaler

	// Roles is a map of message type to role, used to validate the messages
	// that are being packed.
	Roles message.TypeRoles

	// GenerateID is a function used to generate new message IDs. If it is nil,
	// a UUID is generated.
	GenerateID func() string

	// Now is a function used to get the current time. If it is nil, time.Now()
	// is used.
	Now func() time.Time
}

// NewPackerForApplication returns an message packer configured for use with the
// given application.
func NewPackerForApplication(
	cfg configkit.RichApplication,
	ma marshalkit.Marshaler,
) *Packer {
	return &Packer{
		Application: cfg.Identity(),
		Roles:       cfg.MessageTypes().Produced,
		Marshaler:   ma,
	}
}

// PackCommand returns a new command envelope containing the given message.
func (p *Packer) PackCommand(m dogma.Message) *Envelope {
	id := p.generateID()

	return p.new(
		id,
		id,
		id,
		m,
		message.CommandRole,
		configkit.Identity{},
		"",
	)
}

// PackEvent returns a new event envelope containing the given message.
func (p *Packer) PackEvent(m dogma.Message) *Envelope {
	id := p.generateID()

	return p.new(
		id,
		id,
		id,
		m,
		message.EventRole,
		configkit.Identity{},
		"",
	)
}

// PackChildCommand returns a new command envelope containing the given message
// and configured as a child of cause.
func (p *Packer) PackChildCommand(
	cause *Envelope,
	m dogma.Message,
	handler configkit.Identity,
	instanceID string,
) *Envelope {
	mt := message.TypeOf(cause.Message)
	p.Roles[mt].MustBe(message.EventRole, message.TimeoutRole)

	id := p.generateID()

	return p.new(
		id,
		cause.MessageID,
		cause.CorrelationID,
		m,
		message.CommandRole,
		handler,
		instanceID,
	)
}

// PackChildEvent returns a new event envelope containing the given message and
// configured as a child of cause.
func (p *Packer) PackChildEvent(
	cause *Envelope,
	m dogma.Message,
	handler configkit.Identity,
	instanceID string,
) *Envelope {
	mt := message.TypeOf(cause.Message)
	p.Roles[mt].MustBe(message.CommandRole)

	id := p.generateID()

	return p.new(
		id,
		cause.MessageID,
		cause.CorrelationID,
		m,
		message.EventRole,
		handler,
		instanceID,
	)
}

// PackChildTimeout returns a new timeout envelope containing the given message
// and configured as a child of cause.
func (p *Packer) PackChildTimeout(
	cause *Envelope,
	m dogma.Message,
	t time.Time,
	handler configkit.Identity,
	instanceID string,
) *Envelope {
	mt := message.TypeOf(cause.Message)
	p.Roles[mt].MustBe(message.EventRole, message.TimeoutRole)

	id := p.generateID()

	env := p.new(
		id,
		cause.MessageID,
		cause.CorrelationID,
		m,
		message.TimeoutRole,
		handler,
		instanceID,
	)

	env.ScheduledFor = t

	return env
}

// Bind returns a packer bound to the given handler.
func (p *Packer) Bind(
	cause *Envelope,
	cfg configkit.RichHandler,
	instanceID string,
) *BoundPacker {
	if instanceID == "" {
		cfg.HandlerType().MustBe(configkit.IntegrationHandlerType)
	} else {
		cfg.HandlerType().MustBe(configkit.AggregateHandlerType, configkit.ProcessHandlerType)
	}

	return &BoundPacker{
		Packer:        p,
		Cause:         cause,
		HandlerConfig: cfg,
		InstanceID:    instanceID,
	}
}

// new returns a new envelope, it panics if the given message type does not map
// to the r role.
func (p *Packer) new(
	messageID string,
	causationID string,
	correlationID string,
	m dogma.Message,
	r message.Role,
	handler configkit.Identity,
	instanceID string,
) *Envelope {
	mt := message.TypeOf(m)
	p.Roles[mt].MustBe(r)

	return &Envelope{
		MetaData{
			messageID,
			causationID,
			correlationID,
			Source{
				p.Application,
				handler,
				instanceID,
			},
			p.now(),
			time.Time{},
		},
		m,
		marshalkit.MustMarshalMessage(p.Marshaler, m),
	}
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
