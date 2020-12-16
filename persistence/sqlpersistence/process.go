package sqlpersistence

import (
	"context"
	"database/sql"

	"github.com/dogmatiq/verity/persistence"
)

// ProcessDriver is the subset of the Driver interface that is concerned with
// processess.
type ProcessDriver interface {
	// InsertProcessInstance inserts a process instance.
	//
	// It returns false if the row already exists.
	InsertProcessInstance(
		ctx context.Context,
		tx *sql.Tx,
		ak string,
		inst persistence.ProcessInstance,
	) (bool, error)

	// UpdateProcessInstance updates a process instance.
	//
	// It returns false if the row does not exist or inst.Revision is not
	// current.
	UpdateProcessInstance(
		ctx context.Context,
		tx *sql.Tx,
		ak string,
		inst persistence.ProcessInstance,
	) (bool, error)

	// DeleteProcessInstance deletes a process instance.
	//
	// It returns false if the row does not exist or inst.Revision is not
	// current.
	DeleteProcessInstance(
		ctx context.Context,
		tx *sql.Tx,
		ak string,
		inst persistence.ProcessInstance,
	) (bool, error)

	// SelectProcessInstance selects a process instance's data.
	SelectProcessInstance(
		ctx context.Context,
		db *sql.DB,
		ak, hk, id string,
	) (persistence.ProcessInstance, error)
}

// LoadProcessInstance loads a process instance.
//
// hk is the process handler's identity key, id is the instance ID.
func (ds *dataStore) LoadProcessInstance(
	ctx context.Context,
	hk, id string,
) (persistence.ProcessInstance, error) {
	return ds.driver.SelectProcessInstance(
		ctx,
		ds.db,
		ds.appKey,
		hk,
		id,
	)
}

// VisitSaveProcessInstance applies the changes in a "SaveProcessInstance"
// operation to the database.
func (c *committer) VisitSaveProcessInstance(
	ctx context.Context,
	op persistence.SaveProcessInstance,
) error {
	fn := c.driver.InsertProcessInstance
	if op.Instance.Revision > 0 {
		fn = c.driver.UpdateProcessInstance
	}

	ok, err := fn(
		ctx,
		c.tx,
		c.appKey,
		op.Instance,
	)
	if ok || err != nil {
		return err
	}

	return persistence.ConflictError{
		Cause: op,
	}
}

// VisitRemoveProcessInstance applies the changes in a "RemoveProcessInstance"
// operation to the database.
func (c *committer) VisitRemoveProcessInstance(
	ctx context.Context,
	op persistence.RemoveProcessInstance,
) error {
	ok, err := c.driver.DeleteProcessInstance(
		ctx,
		c.tx,
		c.appKey,
		op.Instance,
	)

	if err != nil {
		return err
	}

	if !ok {
		return persistence.ConflictError{
			Cause: op,
		}
	}

	return c.driver.DeleteQueueTimeoutMessagesByProcessInstance(
		ctx,
		c.tx,
		c.appKey,
		op.Instance,
	)
}
