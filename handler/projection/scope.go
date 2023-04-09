package projection

import (
	"time"

	"github.com/dogmatiq/dodeca/logging"
	"github.com/dogmatiq/verity/internal/mlog"
	"github.com/dogmatiq/verity/parcel"
)

// eventScope is an implementation of dogma.ProjectionEventScope.
type eventScope struct {
	cause  parcel.Parcel
	logger logging.Logger
}

// RecordedAt returns the time at which the event was recorded.
func (s eventScope) RecordedAt() time.Time {
	return s.cause.CreatedAt
}

// IsPrimaryDelivery returns true on one of the application instances that
// receive the event, and false on all other instances.
func (s eventScope) IsPrimaryDelivery() bool {
	return true
}

// Log records an informational message within the context of the message
// that is being handled.
func (s eventScope) Log(f string, v ...interface{}) {
	mlog.LogFromScope(
		s.logger,
		s.cause.Envelope,
		f,
		v,
	)
}

// compactScope is an implementation of dogma.ProjectionCompactScope.
type compactScope struct {
	logger logging.Logger
}

// Log records an informational message within the context of compaction.
func (s compactScope) Log(f string, v ...interface{}) {
	logging.Log(s.logger, f, v...)
}

// Now returns the time.
func (s compactScope) Now() time.Time {
	return time.Now()
}
