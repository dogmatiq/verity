package aggregate

import (
	"fmt"

	"github.com/dogmatiq/dodeca/logging"
	"github.com/dogmatiq/dogma"
	"github.com/dogmatiq/infix/draftspecs/envelopespec"
	"github.com/dogmatiq/infix/internal/mlog"
	"github.com/dogmatiq/infix/parcel"
)

// scope is an implementation of dogma.AggregateCommandScope.
type scope struct {
	cause   *parcel.Parcel
	packer  *parcel.Packer
	handler *envelopespec.Identity
	logger  logging.Logger

	id     string
	root   dogma.AggregateRoot
	exists bool

	created   bool
	destroyed bool
	events    []*parcel.Parcel
}

// InstanceID returns the ID of the targeted aggregate instance.
func (s *scope) InstanceID() string {
	return s.id
}

// Create creates the targeted instance.
func (s *scope) Create() bool {
	if s.exists {
		return false
	}

	s.exists = true
	s.created = true

	return true
}

// Destroy destroys the targeted instance.
func (s *scope) Destroy() {
	if !s.exists {
		panic("can not destroy non-existent instance")
	}

	s.exists = false
	s.destroyed = true
}

// Root returns the root of the targeted aggregate instance.
func (s *scope) Root() dogma.AggregateRoot {
	if !s.exists {
		panic("can not access aggregate root of non-existent instance")
	}

	return s.root
}

// RecordEvent records the occurrence of an event as a result of the command
// message that is being handled.
func (s *scope) RecordEvent(m dogma.Message) {
	if !s.exists {
		panic("can not record event against non-existent instance")
	}

	p := s.packer.PackChildEvent(
		s.cause,
		m,
		s.handler,
		s.id,
	)

	s.root.ApplyEvent(m)
	s.events = append(s.events, p)

	mlog.LogProduce(s.logger, p.Envelope)
}

// Log records an informational message within the context of the message
// that is being handled.
func (s *scope) Log(f string, v ...interface{}) {
	mlog.LogFromHandler(
		s.logger,
		s.cause.Envelope,
		f,
		v,
	)
}

// validate panics if the handler left the scope in an invalid state.
func (s *scope) validate() {
	if len(s.events) > 0 {
		return
	}

	if s.created {
		panic(fmt.Sprintf(
			"the '%s' aggregate message handler created the '%s' instance without recording an event while handling a %T command",
			s.handler.Name,
			s.id,
			s.cause.Message,
		))
	}

	if s.destroyed {
		panic(fmt.Sprintf(
			"the '%s' aggregate message handler destroyed the '%s' instance without recording an event while handling a %T command",
			s.handler.Name,
			s.id,
			s.cause.Message,
		))
	}
}
