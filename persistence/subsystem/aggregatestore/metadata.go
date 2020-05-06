package aggregatestore

// MetaData contains meta-data about an aggregate instance.
type MetaData struct {
	// HandlerKey is the identity key of the aggregate message handler.
	HandlerKey string

	// InstanceID is the application-defined instance ID.
	InstanceID string

	// Revision is the instance's version, used to enforce optimistic
	// concurrency control.
	Revision uint64

	// InstanceExists is true if the instance currently exists.
	InstanceExists bool

	// LastDestroyedBy is the ID of the last event message recorded in when the
	// instance was most recently destroyed.
	LastDestroyedBy string

	// BeginOffset specifies the (inclusive) lower-bound of the event offsets
	// that should be considered when loading the instance's historical events.
	//
	// It defaults to zero, indicating that there is no minimum. Note that the
	// event AT this offset was not necessarily recorded by the instance.
	//
	// When an instance is destroyed, BeginOffset is set to EndOffset,
	// preventing any events that were recorded prior destruction from being
	// "seen" by the handler in the future, without actually deleting historical
	// events.
	//
	// TODO: https://github.com/dogmatiq/dogma/issues/220
	// Remove this field.
	BeginOffset uint64

	// EndOffset specifies the (exclusive) upper-bound of the offset range that
	// should be searched when reading the instance's historical events.
	//
	// It defaults to zero, meaning that the range [BeginOffset, EndOffset) is
	// empty, and that the instance does not exist.
	//
	// If non-zero, EndOffset is the offset after the last event recorded by
	// the instance.
	//
	// TODO: https://github.com/dogmatiq/dogma/issues/220
	// Remove this field.
	EndOffset uint64
}

// MarkInstanceDestroyed marks the instance as destroyed.
//
// id is the message ID of the last event recorded when the instance was
// destroyed.
func (md *MetaData) MarkInstanceDestroyed(id string) {
	md.InstanceExists = false
	md.LastDestroyedBy = id
	md.BeginOffset = md.EndOffset
}

// SetLastRecordedOffset updates the meta-data to reflect that o was the offset
// of the most-recent event recorded by the instance.
func (md *MetaData) SetLastRecordedOffset(o uint64) {
	md.InstanceExists = true
	md.EndOffset = o + 1
}
