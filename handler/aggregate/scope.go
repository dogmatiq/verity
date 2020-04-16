package aggregate

import (
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
	id      string
	exists  bool
	root    dogma.AggregateRoot
	logger  logging.Logger
	events  []*parcel.Parcel
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
	return true
}

// Destroy destroys the targeted instance.
func (s *scope) Destroy() {

}

// Root returns the root of the targeted aggregate instance.
func (s *scope) Root() dogma.AggregateRoot {
	// TODO: guard against calls when instance does not exist
	return s.root
}

// RecordEvent records the occurrence of an event as a result of the command
// message that is being handled.
func (s *scope) RecordEvent(m dogma.Message) {
	p := s.packer.PackChildEvent(
		s.cause,
		m,
		s.handler,
		s.id,
	)

	mlog.LogProduce(s.logger, p.Envelope)

	s.events = append(s.events, p)
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
