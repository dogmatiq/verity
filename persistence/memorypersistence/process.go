package memorypersistence

import (
	"context"

	"github.com/dogmatiq/verity/persistence"
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
	c.db.process.save(op.Instance)
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

	c.db.process.remove(key)
	c.db.queue.removeTimeoutsByProcessInstance(key)
	return nil
}

// processDatabase contains process related data.
type processDatabase struct {
	instances map[instanceKey]persistence.ProcessInstance
}

// save stores inst in the database.
func (db *processDatabase) save(inst persistence.ProcessInstance) {
	key := instanceKey{inst.HandlerKey, inst.InstanceID}

	if db.instances == nil {
		db.instances = map[instanceKey]persistence.ProcessInstance{}
	}

	inst.Revision++
	db.instances[key] = inst
}

// remove removes the process instance with the given key.
func (db *processDatabase) remove(key instanceKey) {
	delete(db.instances, key)
}
