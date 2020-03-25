package boltdb

import (
	"context"
	"sync"

	"github.com/dogmatiq/infix/internal/x/bboltx"
	"github.com/dogmatiq/infix/persistence"
	"github.com/dogmatiq/infix/persistence/eventstore"
	"go.etcd.io/bbolt"
	"go.uber.org/multierr"
)

// dataStore is an implementation of persistence.DataStore for BoltDB.
type dataStore struct {
	db     *bbolt.DB
	appKey []byte

	m       sync.Mutex
	pending map[*transaction]struct{}
	closer  func() error
}

func newDataStore(
	db *bbolt.DB,
	k string,
	c func() error,
) *dataStore {
	return &dataStore{
		db:     db,
		appKey: []byte(k),
		closer: c,
	}
}

// EventStoreRepository returns the application's event store repository.
func (ds *dataStore) EventStoreRepository() eventstore.Repository {
	return &eventStoreRepository{ds.db, ds.appKey}
}

// Begin starts a new transaction.
func (ds *dataStore) Begin(ctx context.Context) (_ persistence.Transaction, err error) {
	defer bboltx.Recover(&err)

	ds.m.Lock()
	defer ds.m.Unlock()

	if ds.closer == nil {
		return nil, persistence.ErrDataStoreClosed
	}

	native := bboltx.BeginWrite(ds.db)
	defer func() {
		if err != nil {
			native.Rollback()
		}
	}()

	tx := &transaction{
		tx:     native,
		bucket: bboltx.CreateBucketIfNotExists(native, ds.appKey),
	}

	// Setup a callback to remove the handler from the pending transaction list
	// when it is committed or rolled back.
	tx.remove = func() {
		ds.m.Lock()
		defer ds.m.Unlock()
		delete(ds.pending, tx)
	}

	if ds.pending == nil {
		ds.pending = map[*transaction]struct{}{}
	}

	ds.pending[tx] = struct{}{}

	return tx, nil
}

// Close closes the data store.
func (ds *dataStore) Close() error {
	ds.m.Lock()
	defer ds.m.Unlock()

	if ds.closer == nil {
		return persistence.ErrDataStoreClosed
	}

	// Closing a BoltDB database blocks until all in-flight transactions are
	// committed or rolled back, whereas the persistence.DataStore interface
	// requires that closing the data-store should cause any in-flight
	// transactions to fail.
	//
	// We keep a list of pending transactions so that we can forcefully roll
	// them back when the data-store is closed.
	var err error
	for tx := range ds.pending {
		err = multierr.Append(
			err,
			tx.forceClose(),
		)
	}

	closer := ds.closer
	ds.closer = nil
	ds.pending = nil

	return multierr.Append(
		err,
		closer(),
	)
}
