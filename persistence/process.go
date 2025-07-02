package persistence

import (
	"context"

	"github.com/dogmatiq/enginekit/marshaler"
)

// ProcessInstance contains the state of a process instance.
type ProcessInstance struct {
	// HandlerKey is the identity key of the process message handler.
	HandlerKey string

	// InstanceID is the process instance ID.
	InstanceID string

	// Revision is the instance's current version, used to enforce optimistic
	// concurrency control.
	Revision uint64

	// Packet contains the binary representation of the process state.
	Packet marshaler.Packet

	// HasEnded flags the process as having ended.
	HasEnded bool
}

// ProcessRepository is an interface for reading process state.
type ProcessRepository interface {
	// LoadProcessInstance loads a process instance.
	//
	// hk is the process handler's identity key, id is the instance ID.
	LoadProcessInstance(
		ctx context.Context,
		hk, id string,
	) (ProcessInstance, error)
}

// SaveProcessInstance is an Operation that creates or updates a process
// instance.
type SaveProcessInstance struct {
	// Instance is the instance to persist.
	//
	// Instance.Revision must be the revision of the process instance as
	// currently persisted, otherwise an optimistic concurrency conflict occurs
	// and the entire batch of operations is rejected.
	Instance ProcessInstance
}

// AcceptVisitor calls v.VisitSaveProcessInstance().
func (op SaveProcessInstance) AcceptVisitor(ctx context.Context, v OperationVisitor) error {
	return v.VisitSaveProcessInstance(ctx, op)
}

func (op SaveProcessInstance) entityKey() entityKey {
	return entityKey{"handler", op.Instance.HandlerKey, op.Instance.InstanceID}
}
