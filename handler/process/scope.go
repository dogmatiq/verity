package process

import (
	"time"

	"github.com/dogmatiq/dodeca/logging"
	"github.com/dogmatiq/dogma"
	"github.com/dogmatiq/interopspec/envelopespec"
	"github.com/dogmatiq/verity/handler"
	"github.com/dogmatiq/verity/internal/mlog"
	"github.com/dogmatiq/verity/parcel"
)

// scope is an implementation of both dogma.ProcessEventScope and
// dogma.ProcessTimeoutScope. It is the application-developer-facing interface
// to a UnitOfWork.
type scope struct {
	identity   *envelopespec.Identity
	handler    dogma.ProcessMessageHandler
	packer     *parcel.Packer
	logger     logging.Logger
	work       handler.UnitOfWork
	cause      parcel.Parcel
	instanceID string
	timeouts   []parcel.Parcel
	ended      bool // true if End() has been called successfully at least once
}

// InstanceID returns the ID of the targeted process instance.
func (s *scope) InstanceID() string {
	return s.instanceID
}

// End terminates the targeted process instance.
func (s *scope) End() {
	s.ended = true
}

// ExecuteCommand executes a command as a result of the event or timeout
// message being handled.
func (s *scope) ExecuteCommand(m dogma.Command) {
	p := s.packer.PackChildCommand(
		s.cause,
		m,
		s.identity,
		s.instanceID,
	)

	s.ended = false
	s.work.ExecuteCommand(p)

	mlog.LogProduce(s.logger, p.Envelope)
}

// ScheduleTimeout schedules a timeout message to be handled by this process
// instance at a specific time.
func (s *scope) ScheduleTimeout(m dogma.Timeout, t time.Time) {
	p := s.packer.PackChildTimeout(
		s.cause,
		m,
		t,
		s.identity,
		s.instanceID,
	)

	s.ended = false
	s.timeouts = append(s.timeouts, p)

	mlog.LogProduce(s.logger, p.Envelope)
}

// RecordedAt returns the time at which the event was recorded.
func (s *scope) RecordedAt() time.Time {
	return s.cause.CreatedAt
}

// ScheduledFor returns the time at which the timeout message was scheduled
// to be handled.
func (s *scope) ScheduledFor() time.Time {
	return s.cause.ScheduledFor
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
