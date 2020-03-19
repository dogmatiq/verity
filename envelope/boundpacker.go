package envelope

import (
	"fmt"
	"time"

	"github.com/dogmatiq/configkit"
	"github.com/dogmatiq/dogma"
)

// BoundPacker is an envelope packer that's bound to a specific source handler.
type BoundPacker struct {
	Packer        *Packer
	Cause         *Envelope
	HandlerConfig configkit.RichHandler
	InstanceID    string
}

// PackChildCommand returns a new command envelope containing the given message
// and configured as a child of cause.
func (p *BoundPacker) PackChildCommand(m dogma.Message) *Envelope {
	p.HandlerConfig.HandlerType().MustBe(
		configkit.ProcessHandlerType,
	)

	env := p.Packer.PackChildCommand(
		p.Cause,
		m,
		p.HandlerConfig.Identity(),
		p.InstanceID,
	)

	if p.HandlerConfig.MessageTypes().Produced.HasM(m) {
		return env
	}

	panic(fmt.Sprintf(
		"the '%s' handler is not configured to produced commands of type %T",
		p.HandlerConfig.Identity().Name,
		m,
	))
}

// PackChildEvent returns a new event envelope containing the given message and
// configured as a child of cause.
func (p *BoundPacker) PackChildEvent(m dogma.Message) *Envelope {
	p.HandlerConfig.HandlerType().MustBe(
		configkit.AggregateHandlerType,
		configkit.IntegrationHandlerType,
	)

	env := p.Packer.PackChildEvent(
		p.Cause,
		m,
		p.HandlerConfig.Identity(),
		p.InstanceID,
	)

	if p.HandlerConfig.MessageTypes().Produced.HasM(m) {
		return env
	}

	panic(fmt.Sprintf(
		"the '%s' handler is not configured to produced events of type %T",
		p.HandlerConfig.Identity().Name,
		m,
	))
}

// PackChildTimeout returns a new timeout envelope containing the given message
// and configured as a child of cause.
func (p *BoundPacker) PackChildTimeout(m dogma.Message, t time.Time) *Envelope {
	p.HandlerConfig.HandlerType().MustBe(
		configkit.ProcessHandlerType,
	)

	env := p.Packer.PackChildTimeout(
		p.Cause,
		m,
		t,
		p.HandlerConfig.Identity(),
		p.InstanceID,
	)

	if p.HandlerConfig.MessageTypes().Produced.HasM(m) {
		return env
	}

	panic(fmt.Sprintf(
		"the '%s' handler is not configured to produced events of type %T",
		p.HandlerConfig.Identity().Name,
		m,
	))
}
