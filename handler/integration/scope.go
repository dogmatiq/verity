package integration

import (
	"github.com/dogmatiq/dodeca/logging"
	"github.com/dogmatiq/dogma"
	"github.com/dogmatiq/envelopespec"
	"github.com/dogmatiq/infix/handler"
	"github.com/dogmatiq/infix/internal/mlog"
	"github.com/dogmatiq/infix/parcel"
)

// scope is an implementation of dogma.IntegrationEventScope. It is the
// application-developer-facing interface to a UnitOfWork.
type scope struct {
	identity *envelopespec.Identity
	packer   *parcel.Packer
	logger   logging.Logger
	work     handler.UnitOfWork
	cause    parcel.Parcel
}

// RecordEvent records the occurrence of an event as a result of the command
// message that is being handled.
func (s *scope) RecordEvent(m dogma.Message) {
	p := s.packer.PackChildEvent(
		s.cause,
		m,
		s.identity,
		"",
	)

	s.work.RecordEvent(p)

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
