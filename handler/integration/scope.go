package integration

import (
	"github.com/dogmatiq/dodeca/logging"
	"github.com/dogmatiq/dogma"
	"github.com/dogmatiq/infix/draftspecs/envelopespec"
	"github.com/dogmatiq/infix/envelope"
	"github.com/dogmatiq/infix/internal/mlog"
)

// scope is an implementation of dogma.IntegrationEventScope.
type scope struct {
	envelope *envelopespec.Envelope
	message  dogma.Message
	handler  *envelopespec.Identity
	packer   *envelope.Packer
	logger   logging.Logger
	events   []*envelopespec.Envelope
}

// RecordEvent records the occurrence of an event as a result of the command
// message that is being handled.
func (s *scope) RecordEvent(m dogma.Message) {
	env := s.packer.PackChildEvent(
		s.envelope,
		s.message,
		m,
		s.handler,
		"",
	)

	mlog.LogProduce(s.logger, env)

	s.events = append(s.events, env)
}

// Log records an informational message within the context of the message
// that is being handled.
func (s *scope) Log(f string, v ...interface{}) {
	mlog.LogFromHandler(
		s.logger,
		s.envelope,
		f,
		v,
	)
}
