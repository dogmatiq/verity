package process

import (
	"time"

	"github.com/dogmatiq/dodeca/logging"
	"github.com/dogmatiq/dogma"
	"github.com/dogmatiq/infix/draftspecs/envelopespec"
	"github.com/dogmatiq/infix/handler"
	"github.com/dogmatiq/infix/internal/mlog"
	"github.com/dogmatiq/infix/parcel"
)

// scope is an implementation of both dogma.ProcessEventScope and
// dogma.ProcessTimeoutScope. It is the application-developer-facing interface
// to a UnitOfWork.
type scope struct {
	identity *envelopespec.Identity
	handler  dogma.ProcessMessageHandler
	packer   *parcel.Packer
	logger   logging.Logger
	work     handler.UnitOfWork
	cause    parcel.Parcel
	instance *Instance
	exists   bool // true if the instance currently exists
	begun    bool // true if Begin() has been called successfully at least once
	ended    bool // true if End() has been called successfully at least once
}

// InstanceID returns the ID of the targeted process instance.
func (s *scope) InstanceID() string {
	return s.instance.InstanceID
}

// Begin starts the targeted process instance.
func (s *scope) Begin() bool {
	if s.exists {
		return false
	}

	s.exists = true
	s.begun = true

	return true
}

// End terminates the targeted process instance.
func (s *scope) End() {
	if !s.exists {
		panic("can not end a process instance that has not begun")
	}

	s.instance.Root = mustNew(s.handler)
	s.ended = true
}

// Root returns the root of the targeted process instance.
func (s *scope) Root() dogma.ProcessRoot {
	if !s.exists {
		panic("can not access the root of a process instance that has not begun")
	}

	return s.instance.Root
}

// ExecuteCommand executes a command as a result of the event or timeout
// message being handled.
func (s *scope) ExecuteCommand(m dogma.Message) {
	if !s.exists {
		panic("can not execute a command within a process instance that has not begun")
	}

	p := s.packer.PackChildCommand(
		s.cause,
		m,
		s.identity,
		s.instance.InstanceID,
	)

	s.work.ExecuteCommand(p)

	mlog.LogProduce(s.logger, p.Envelope)
}

// ScheduleTimeout schedules a timeout message to be handled by this process
// instance at a specific time.
func (s *scope) ScheduleTimeout(m dogma.Message, t time.Time) {
	if !s.exists {
		panic("can not schedule a timeout within a process instance that has not begun")
	}

	p := s.packer.PackChildTimeout(
		s.cause,
		m,
		t,
		s.identity,
		s.instance.InstanceID,
	)

	s.work.ScheduleTimeout(p)

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
