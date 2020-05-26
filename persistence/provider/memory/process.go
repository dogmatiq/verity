package memory

import (
	"context"
	"errors"

	"github.com/dogmatiq/infix/persistence"
)

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
