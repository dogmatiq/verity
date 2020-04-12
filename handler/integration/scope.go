package integration

import (
	"fmt"

	"github.com/dogmatiq/configkit"
	"github.com/dogmatiq/dodeca/logging"
	"github.com/dogmatiq/dogma"
	"github.com/dogmatiq/infix/envelope"
	"github.com/dogmatiq/infix/internal/mlog"
)

// scope is an implementation of dogma.IntegrationEventScope.
type scope struct {
	handler configkit.Identity
	cause   *envelope.Envelope
	packer  *envelope.Packer
	logger  logging.Logger
	events  []*envelope.Envelope
}

// RecordEvent records the occurrence of an event as a result of the command
// message that is being handled.
func (s *scope) RecordEvent(m dogma.Message) {
	s.events = append(
		s.events,
		s.packer.PackChildEvent(
			s.cause,
			m,
			s.handler,
			"",
		),
	)
}

// Log records an informational message within the context of the message
// that is being handled.
func (s *scope) Log(f string, v ...interface{}) {
	logging.Log(
		s.logger,
		mlog.String(
			[]mlog.IconWithLabel{
				mlog.MessageIDIcon.WithID(s.cause.MessageID),
				mlog.CausationIDIcon.WithID(s.cause.CausationID),
				mlog.CorrelationIDIcon.WithID(s.cause.CorrelationID),
			},
			[]mlog.Icon{
				mlog.InboundIcon,
				mlog.IntegrationIcon,
			},
			s.handler.Name,
			fmt.Sprintf(f, v...),
		),
	)
}
