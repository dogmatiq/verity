package aggregate

import (
	"fmt"

	"github.com/dogmatiq/dodeca/logging"
	"github.com/dogmatiq/dogma"
	"github.com/dogmatiq/envelopespec"
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
	return s.instance.InstanceID
}

// Exists returns true if the aggregate instance exists.
func (s *scope) Exists() bool {
	return s.instance.InstanceExists
}

// Create creates the targeted instance.
func (s *scope) Create() bool {
	if s.instance.InstanceExists {
		return false
	}

	if s.destroyed {
		panic("can not create an instance that was destroyed by the same message")
	}

	s.instance.InstanceExists = true
	s.created = true

	return true
}

// Destroy destroys the targeted instance.
func (s *scope) Destroy() {
	if !s.instance.InstanceExists {
		panic("can not destroy an aggregate instance that has not been created")
	}

	s.instance.Root = mustNew(s.handler)
	s.instance.InstanceExists = false
	s.instance.LastDestroyedBy = s.lastEventID
	s.destroyed = true
}

// Root returns the root of the targeted aggregate instance.
func (s *scope) Root() dogma.AggregateRoot {
	if !s.instance.InstanceExists {
		panic("can not access the root of an aggregate instance that has not been created")
	}

	return s.instance.Root
}

// RecordEvent records the occurrence of an event as a result of the command
// message that is being handled.
func (s *scope) RecordEvent(m dogma.Message) {
	if !s.instance.InstanceExists {
		panic("can not record an event against an aggregate instance that has not been created")
	}

	p := s.packer.PackChildEvent(
		s.cause,
		m,
		s.identity,
		s.instance.InstanceID,
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

// finalize panics if the handler left the instance in an invalid state.
func (s *scope) finalize() {
	if s.lastEventID != "" {
		return
	}

	if s.created {
		panic(fmt.Sprintf(
			"%T.HandleEvent() created the '%s' instance without recording an event while handling a %T command",
			s.handler,
			s.instance.InstanceID,
			s.cause.Message,
		))
	}

	if s.destroyed {
		panic(fmt.Sprintf(
			"%T.HandleEvent() destroyed the '%s' instance without recording an event while handling a %T command",
			s.handler,
			s.instance.InstanceID,
			s.cause.Message,
		))
	}
}
