package integration

import (
	"github.com/dogmatiq/dodeca/logging"
	"github.com/dogmatiq/dogma"
	"github.com/dogmatiq/infix/draftspecs/envelopespec"
	"github.com/dogmatiq/infix/internal/mlog"
	"github.com/dogmatiq/infix/parcel"
)

// scope is an implementation of dogma.IntegrationEventScope.
type scope struct {
	cause   *parcel.Parcel
	packer  *parcel.Packer
	handler *envelopespec.Identity
	logger  logging.Logger
	events  []*parcel.Parcel
}

// RecordEvent records the occurrence of an event as a result of the command
// message that is being handled.
func (s *scope) RecordEvent(m dogma.Message) {
	p := s.packer.PackChildEvent(
		s.cause,
		m,
		s.handler,
		"",
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
