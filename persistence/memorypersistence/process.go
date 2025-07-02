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

// VisitSaveProcessInstance applies the changes in a "SaveProcessInstance"
// operation to the database.
func (c *committer) VisitSaveProcessInstance(
	ctx context.Context,
	op persistence.SaveProcessInstance,
) error {
	inst := op.Instance
	c.db.process.save(inst)

	if inst.HasEnded {
		key := instanceKey{inst.HandlerKey, inst.InstanceID}
		c.db.queue.removeTimeoutsByProcessInstance(key)
	}

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
