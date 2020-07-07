package memory

import (
	"context"
	"errors"

	"github.com/dogmatiq/infix/persistence"
)

// LoadProcessInstance loads a process instance.
//
// hk is the process handler's identity key, id is the instance ID.
func (ds *dataStore) LoadProcessInstance(
	ctx context.Context,
	hk, id string,
) (persistence.ProcessInstance, error) {
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
	return errors.New("not implemented")
}

// VisitRemoveProcessInstance returns an error if a "RemoveProcessInstance"
// operation can not be applied to the database.
func (v *validator) VisitRemoveProcessInstance(
	ctx context.Context,
	op persistence.RemoveProcessInstance,
) error {
	return errors.New("not implemented")
}

// VisitSaveProcessInstance applies the changes in a "SaveProcessInstance"
// operation to the database.
func (c *committer) VisitSaveProcessInstance(
	ctx context.Context,
	op persistence.SaveProcessInstance,
) error {
	return errors.New("not implemented")
}

// VisitRemoveProcessInstance applies the changes in a "RemoveProcessInstance"
// operation to the database.
func (c *committer) VisitRemoveProcessInstance(
	ctx context.Context,
	op persistence.RemoveProcessInstance,
) error {
	return errors.New("not implemented")
}
