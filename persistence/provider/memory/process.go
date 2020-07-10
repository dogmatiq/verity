package memory

import (
	"context"

	"github.com/dogmatiq/infix/persistence"
)

// LoadProcessInstance loads a process instance.
//
// hk is the process handler's identity key, id is the instance ID.
func (ds *dataStore) LoadProcessInstance(
	ctx context.Context,
	hk, id string,
) (persistence.ProcessInstance, error) {
	ds.db.mutex.RLock()
	defer ds.db.mutex.RUnlock()

	key := instanceKey{hk, id}
	if inst, ok := ds.db.process.instances[key]; ok {
		return inst, nil
	}

	return persistence.ProcessInstance{
		HandlerKey: hk,
		InstanceID: id,
	}, nil
}

// processDatabase contains process related data.
type processDatabase struct {
	instances map[instanceKey]persistence.ProcessInstance
}

// VisitSaveProcessInstance returns an error if a "SaveProcessInstance"
// operation can not be applied to the database.
func (v *validator) VisitSaveProcessInstance(
	_ context.Context,
	op persistence.SaveProcessInstance,
) error {
	new := op.Instance
	key := instanceKey{new.HandlerKey, new.InstanceID}
	old := v.db.process.instances[key]

	if new.Revision == old.Revision {
		return nil
	}

	return persistence.ConflictError{
		Cause: op,
	}
}

// VisitRemoveProcessInstance returns an error if a "RemoveProcessInstance"
// operation can not be applied to the database.
func (v *validator) VisitRemoveProcessInstance(
	ctx context.Context,
	op persistence.RemoveProcessInstance,
) error {
	inst := op.Instance
	key := instanceKey{inst.HandlerKey, inst.InstanceID}

	if x, ok := v.db.process.instances[key]; ok {
		if inst.Revision == x.Revision {
			return nil
		}
	}

	return persistence.ConflictError{
		Cause: op,
	}
}

// VisitSaveProcessInstance applies the changes in a "SaveProcessInstance"
// operation to the database.
func (c *committer) VisitSaveProcessInstance(
	ctx context.Context,
	op persistence.SaveProcessInstance,
) error {
	inst := op.Instance
	key := instanceKey{inst.HandlerKey, inst.InstanceID}

	if c.db.process.instances == nil {
		c.db.process.instances = map[instanceKey]persistence.ProcessInstance{}
	}

	inst.Revision++
	c.db.process.instances[key] = inst

	return nil
}

// VisitRemoveProcessInstance applies the changes in a "RemoveProcessInstance"
// operation to the database.
func (c *committer) VisitRemoveProcessInstance(
	ctx context.Context,
	op persistence.RemoveProcessInstance,
) error {
	inst := op.Instance
	key := instanceKey{inst.HandlerKey, inst.InstanceID}

	delete(c.db.process.instances, key)

	return nil
}
