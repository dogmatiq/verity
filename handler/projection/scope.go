package projection

import (
	"time"

	"github.com/dogmatiq/dodeca/logging"
	"github.com/dogmatiq/verity/internal/mlog"
	"github.com/dogmatiq/verity/parcel"
)

// eventScope is an implementation of dogma.ProjectionEventScope.
type eventScope struct {
	cause              parcel.Parcel
	checkpoint, offset uint64
	logger             logging.Logger
}

func (s eventScope) StreamID() string {
	return s.cause.Envelope.GetSourceApplication().GetKey().AsString()
}

func (s eventScope) CheckpointOffset() uint64 {
	return s.checkpoint
}

func (s eventScope) Offset() uint64 {
	return s.offset
}

// RecordedAt returns the time at which the event was recorded.
func (s eventScope) RecordedAt() time.Time {
	return s.cause.CreatedAt
}

// Now returns the current local time according to the engine.
func (s eventScope) Now() time.Time {
	return time.Now()
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

// Now returns the current local time according to the engine.
func (s compactScope) Now() time.Time {
	return time.Now()
}

// Log records an informational message within the context of compaction.
func (s compactScope) Log(f string, v ...interface{}) {
	logging.Log(s.logger, f, v...)
}
