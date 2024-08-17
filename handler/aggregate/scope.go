package aggregate

import (
	"github.com/dogmatiq/dodeca/logging"
	"github.com/dogmatiq/dogma"
	"github.com/dogmatiq/interopspec/envelopespec"
	"github.com/dogmatiq/verity/handler"
	"github.com/dogmatiq/verity/internal/mlog"
	"github.com/dogmatiq/verity/parcel"
)

// scope is an implementation of dogma.AggregateCommandScope. It is the
// application-developer-facing interface to a UnitOfWork.
type scope struct {
	identity  *envelopespec.Identity
	handler   dogma.AggregateMessageHandler
	packer    *parcel.Packer
	logger    logging.Logger
	work      handler.UnitOfWork
	cause     parcel.Parcel
	instance  *Instance // the aggregate instance, this is always within a cache record
	changed   bool      // true if RecordEvent() has been called, or Destroy() succeeded
	destroyed bool      // true if Destroy() has been called without subsequent call to RecordEvent()
}

// InstanceID returns the ID of the targeted aggregate instance.
func (s *scope) InstanceID() string {
	return s.instance.InstanceID
}

// Destroy destroys the targeted instance.
func (s *scope) Destroy() {
	if !s.instance.InstanceExists {
		return
	}

	s.instance.InstanceExists = false
	s.changed = true
	s.destroyed = true
}

// RecordEvent records the occurrence of an event as a result of the command
// message that is being handled.
func (s *scope) RecordEvent(m dogma.Event) {
	p := s.packer.PackChildEvent(
		s.cause,
		m,
		s.identity,
		s.instance.InstanceID,
	)

	s.instance.Root.ApplyEvent(m)
	s.work.RecordEvent(p)
	s.instance.InstanceExists = true
	s.instance.LastEventID = p.ID()
	s.changed = true
	s.destroyed = false

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
