package process

import (
	"time"

	"github.com/dogmatiq/dodeca/logging"
	"github.com/dogmatiq/dogma"
	"github.com/dogmatiq/envelopespec"
	"github.com/dogmatiq/verity/handler"
	"github.com/dogmatiq/verity/internal/mlog"
	"github.com/dogmatiq/verity/parcel"
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
	timeouts []parcel.Parcel
	exists   bool // true if the instance currently exists
	ended    bool // true if End() has been called successfully at least once
}

// InstanceID returns the ID of the targeted process instance.
func (s *scope) InstanceID() string {
	return s.instance.InstanceID
}

// HasBegun returns true if the process has begun.
func (s *scope) HasBegun() bool {
	return s.exists
}

// Begin starts the targeted process instance.
func (s *scope) Begin() bool {
	if s.exists {
		return false
	}

	if s.ended {
		panic("can not begin an instance that was ended by the same message")
	}

	s.exists = true

	return true
}

// End terminates the targeted process instance.
func (s *scope) End() {
	if !s.exists {
		panic("can not end a process instance that has not begun")
	}

	s.exists = false
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

// finalize adds any scheduled timeouts to the unit-of-work if the instance
// still exists.
func (s *scope) finalize() {
	if s.exists {
		for _, p := range s.timeouts {
			s.work.ScheduleTimeout(p)
		}
	}
}
