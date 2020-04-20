package aggregatestore

import "github.com/dogmatiq/infix/persistence/subsystem/eventstore"

// Revision is the version of an aggregate instance, used for optimistic
// concurrency control.
type Revision uint64

// MetaData contains meta-data about an aggregate instance.
type MetaData struct {
	// HandlerKey is the identity key of the aggregate message handler.
	HandlerKey string

	// InstanceID is the application-defined instance ID.
	InstanceID string

	// Revision is the instance's version, used to enforce optimistic
	// concurrency control.
	Revision Revision

	// MinOffset specifies the (inclusive) lower-bound of the offset range that
	// should be searched when reading this instance's historical events. The
	// event at this offset was NOT necessarily recorded byo this instance.
	MinOffset eventstore.Offset

	// MaxOffset specifies the (exclusive) upper-bound of the offset range that
	// should be searched when reading this instance's historical events.
	//
	// If non-zero, the event at MaxOffset - 1 is the most recent event recorded
	// by this instance.
	MaxOffset eventstore.Offset
}
