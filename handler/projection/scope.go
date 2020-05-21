package projection

import (
	"time"

	"github.com/dogmatiq/dodeca/logging"
	"github.com/dogmatiq/infix/internal/mlog"
	"github.com/dogmatiq/infix/parcel"
)

// scope is an implementation of dogma.ProjectionEventScope.
type scope struct {
	cause  parcel.Parcel
	logger logging.Logger
}

// RecordedAt returns the time at which the event was recorded.
func (s scope) RecordedAt() time.Time {
	return s.cause.CreatedAt
}

// Log records an informational message within the context of the message
// that is being handled.
func (s scope) Log(f string, v ...interface{}) {
	mlog.LogFromScope(
		s.logger,
		s.cause.Envelope,
		f,
		v,
	)
}
