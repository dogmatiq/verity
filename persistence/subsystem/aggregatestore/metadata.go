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
}
