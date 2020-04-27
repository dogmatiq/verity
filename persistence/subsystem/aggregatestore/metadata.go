package aggregatestore

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
	BeginOffset uint64

	// EndOffset specifies the (exclusive) upper-bound of the offset range that
	// should be searched when reading the instance's historical events.
	//
	// It defaults to zero, meaning that the range [BeginOffset, EndOffset) is
	// empty, and that the instance does not exist.
	//
	// If non-zero, EndOffset is the offset after the last event recorded by
	// the instance.
	EndOffset uint64
}

// InstanceExists returns true if the instance exists.
func (md *MetaData) InstanceExists() bool {
	return md.BeginOffset < md.EndOffset
}

// MarkInstanceDestroyed marks the instance as destroyed by moving BeginOffset
// after the offset of the last-recorded event.
func (md *MetaData) MarkInstanceDestroyed() {
	md.BeginOffset = md.EndOffset
}

// SetLastRecordedOffset updates the meta-data to reflect that o was the offset
// of the most-recent event recorded by the instance.
func (md *MetaData) SetLastRecordedOffset(o uint64) {
	md.EndOffset = o + 1
}
