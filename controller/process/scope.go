package process

import (
	"time"

	"github.com/dogmatiq/dodeca/logging"
	"github.com/dogmatiq/dogma"
	"github.com/dogmatiq/infix/envelope"
)

// scope is an implementation of both dogma.ProcessEventScope and
// dogma.ProcessTimeoutScope.
type scope struct {
	packer *envelope.BoundPacker
	logger logging.Logger

	root   dogma.ProcessRoot
	rev    uint64
	exists bool

	commands []*envelope.Envelope
	timeouts []*envelope.Envelope
}

// InstanceID returns the ID of the targeted process instance.
func (s *scope) InstanceID() string {
	return s.packer.InstanceID
}

// Begin starts the targeted process instance.
//
// It MUST be called before Root(), ExecuteCommand() or ScheduleTimeout()
// can be called within this scope or the scope of any future message that
// targets the same instance.
//
// It returns true if the targeted instance was begun, or false if
// the instance had already begun.
func (s *scope) Begin() bool {
	if s.exists {
		return false
	}

	s.exists = true

	return true
}

// End terminates the targeted process instance.
//
// After it has been called Root(), ExecuteCommand() and ScheduleTimeout()
// MUST NOT be called within this scope or the scope of any future message
// that targets the same instance.
//
// It MUST NOT be called if the instance has not begun.
//
// The engine MUST discard any timeout messages associated with this
// instance.
//
// The engine MAY allow re-beginning a process instance that has ended.
// Callers SHOULD assume that such behavior is unavailable.
func (s *scope) End() {
	if !s.exists {
		panic("can not end non-existent instance")
	}

	s.exists = false
	s.timeouts = nil
}

// Root returns the root of the targeted process instance.
//
// It MUST NOT be called if the instance has not begun, or has ended.
func (s *scope) Root() dogma.ProcessRoot {
	if !s.exists {
		panic("can not access process root of non-existent instance")
	}

	return s.root
}

// ExecuteCommand executes a command as a result of the event or timeout
// message being handled.
//
// It MUST NOT be called with a message of any type that has not been
// configured for production by a prior call to Configure().
//
// It MUST NOT be called if the instance has not begun, or has ended.
func (s *scope) ExecuteCommand(m dogma.Message) {
	if !s.exists {
		panic("can not execute command against non-existent instance")
	}

	s.commands = append(
		s.commands,
		s.packer.PackChildCommand(m),
	)
}

// ScheduleTimeout schedules a timeout message to be handled by this process
// instance at a specific time.
//
// Any pending timeout messages are cancelled when the instance is ended.
//
// It MUST NOT be called if the instance has not begun, or has ended.
func (s *scope) ScheduleTimeout(m dogma.Message, t time.Time) {
	if !s.exists {
		panic("can not schedule timeout against non-existent instance")
	}

	s.timeouts = append(
		s.timeouts,
		s.packer.PackChildTimeout(m, t),
	)
}

// RecordedAt returns the time at which the event was recorded.
func (s *scope) RecordedAt() time.Time {
	return s.packer.Cause.CreatedAt
}

// ScheduledFor returns the time at which the timeout was scheduled to occur.
func (s *scope) ScheduledFor() time.Time {
	return s.packer.Cause.ScheduledFor
}

// Log records an informational message within the context of the message
// that is being handled.
func (s *scope) Log(f string, v ...interface{}) {
	logging.Log(s.logger, f, v...)
}
