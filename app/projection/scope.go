package projection

import (
	"time"

	"github.com/dogmatiq/dodeca/logging"
)

// scope is an implementation of dogma.ProjectionEventScope.
type scope struct {
	recordedAt time.Time
	logger     logging.Logger
}

// RecordedAt returns the time at which the event was recorded.
func (s scope) RecordedAt() time.Time {
	return s.recordedAt
}

// Log records an informational message within the context of the message
// that is being handled.
func (s scope) Log(f string, v ...interface{}) {
	logging.Log(s.logger, f, v...)
}
