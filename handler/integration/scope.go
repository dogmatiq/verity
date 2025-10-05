package integration

import (
	"time"

	"github.com/dogmatiq/dodeca/logging"
	"github.com/dogmatiq/dogma"
	"github.com/dogmatiq/interopspec/envelopespec"
	"github.com/dogmatiq/verity/handler"
	"github.com/dogmatiq/verity/internal/mlog"
	"github.com/dogmatiq/verity/parcel"
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
func (s *scope) RecordEvent(m dogma.Event) {
	p := s.packer.PackChildEvent(
		s.cause,
		m,
		s.identity,
		"",
	)

	s.work.RecordEvent(p)

	mlog.LogProduce(s.logger, p.Envelope)
}

// Now returns the current local time according to the engine.
func (s *scope) Now() time.Time {
	return time.Now()
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
