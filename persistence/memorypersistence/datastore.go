package memorypersistence

import (
	"context"
	"sync/atomic"

	"github.com/dogmatiq/verity/persistence"
)

// dataStore is an implementation of persistence.DataStore for the in-memory
// persistence provider.
type dataStore struct {
	db     *database
	closed uint32 // atomic
}

// newDataStore returns a new data-store.
func newDataStore(db *database) *dataStore {
	return &dataStore{
		db: db,
	}
}

// Persist commits a batch of operations atomically.
//
// If any one of the operations causes an optimistic concurrency conflict
// the entire batch is aborted and a ConflictError is returned.
func (ds *dataStore) Persist(
	ctx context.Context,
	b persistence.Batch,
) (persistence.Result, error) {
	b.MustValidate()

	if atomic.LoadUint32(&ds.closed) != 0 {
		return persistence.Result{}, persistence.ErrDataStoreClosed
	}

	ds.db.mutex.Lock()
	defer ds.db.mutex.Unlock()

	v := &validator{ds.db}
	if err := b.AcceptVisitor(ctx, v); err != nil {
		return persistence.Result{}, err
	}

	c := &committer{db: ds.db}
	err := b.AcceptVisitor(ctx, c)

	return c.result, err
}

// Close closes the data store.
//
// Closing a data-store causes any future calls to Persist() to return
// ErrDataStoreClosed.
//
// The behavior read operations on a closed data-store is
// implementation-defined.
//
// In general use it is expected that all pending calls to Persist() will
// have finished before a data-store is closed. Close() may block until any
// in-flight calls to Persist() return, or may prevent any such calls from
// succeeding.
func (ds *dataStore) Close() error {
	if !atomic.CompareAndSwapUint32(&ds.closed, 0, 1) {
		return persistence.ErrDataStoreClosed
	}

	ds.db.Close()

	return nil
}
