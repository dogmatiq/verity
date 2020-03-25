package process

import "github.com/dogmatiq/marshalkit"

// Revision is the "version" of a process instance, used to enforce optimistic
// concurrency constraints.
type Revision uint64

// ID uniquely identifies a process instance.
type ID struct {
	HandlerKey string
	InstanceID string
}

// Instance is a persisted process instance.
type Instance struct {
	ID       ID
	Revision uint64
	Root     marshalkit.Packet
}
