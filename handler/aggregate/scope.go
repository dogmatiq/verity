package aggregate

import (
	"fmt"

	"github.com/dogmatiq/dodeca/logging"
	"github.com/dogmatiq/dogma"
	"github.com/dogmatiq/infix/draftspecs/envelopespec"
	"github.com/dogmatiq/infix/handler"
	"github.com/dogmatiq/infix/internal/mlog"
	"github.com/dogmatiq/infix/parcel"
)

// scope is an implementation of dogma.AggregateCommandScope. It is the
// application-developer-facing interface to a UnitOfWork.
type scope struct {
	identity    *envelopespec.Identity
	handler     dogma.AggregateMessageHandler
	packer      *parcel.Packer
	logger      logging.Logger
	work        handler.UnitOfWork
	cause       parcel.Parcel
	instance    *Instance // the aggregate instance, this is always within a cache record
	created     bool      // true if Create() has been called successfully at least once
	destroyed   bool      // true if Destroy() has been called successfully at least once
	lastEventID string    // ID of the most-recently recorded event
}

// InstanceID returns the ID of the targeted aggregate instance.
func (s *scope) InstanceID() string {
	return s.instance.MetaData.InstanceID
}

// Create creates the targeted instance.
func (s *scope) Create() bool {
	if s.instance.MetaData.InstanceExists {
		return false
	}

	s.instance.MetaData.InstanceExists = true
	s.created = true

	return true
}

// Destroy destroys the targeted instance.
func (s *scope) Destroy() {
	if !s.instance.MetaData.InstanceExists {
		panic("can not destroy non-existent instance")
	}

	s.instance.Root = mustNew(s.handler)
	s.instance.MetaData.InstanceExists = false
	s.instance.MetaData.LastDestroyedBy = s.lastEventID

	s.destroyed = true
}

// Root returns the root of the targeted aggregate instance.
func (s *scope) Root() dogma.AggregateRoot {
	if !s.instance.MetaData.InstanceExists {
		panic("can not access aggregate root of non-existent instance")
	}

	return s.instance.Root
}

// RecordEvent records the occurrence of an event as a result of the command
// message that is being handled.
func (s *scope) RecordEvent(m dogma.Message) {
	if !s.instance.MetaData.InstanceExists {
		panic("can not record event against non-existent instance")
	}

	p := s.packer.PackChildEvent(
		s.cause,
		m,
		s.identity,
		s.instance.MetaData.InstanceID,
	)

	s.instance.Root.ApplyEvent(m)
	s.work.RecordEvent(p)
	s.lastEventID = p.ID()

	mlog.LogProduce(s.logger, p.Envelope)
}

// Log records an informational message within the context of the message
// that is being handled.
func (s *scope) Log(f string, v ...interface{}) {
	mlog.LogFromScope(
		s.logger,
		s.cause.Envelope,
		f,
		v,
	)
}

// validate panics if the handler left the instance in an invalid state.
func (s *scope) validate() {
	if s.lastEventID != "" {
		return
	}

	if s.created {
		panic(fmt.Sprintf(
			"%T.HandleEvent() created the '%s' instance without recording an event while handling a %T command",
			s.handler,
			s.instance.MetaData.InstanceID,
			s.cause.Message,
		))
	}

	if s.destroyed {
		panic(fmt.Sprintf(
			"%T.HandleEvent() destroyed the '%s' instance without recording an event while handling a %T command",
			s.handler,
			s.instance.MetaData.InstanceID,
			s.cause.Message,
		))
	}
}
