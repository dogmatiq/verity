package boltdb

import (
	"context"
	"errors"

	"github.com/dogmatiq/infix/internal/x/bboltx"
	"github.com/dogmatiq/infix/persistence/subsystem/aggregatestore"
	"go.etcd.io/bbolt"
)

var (
	// aggregateStoreBucketKey is the key for the bucket at the root of the
	// aggregatestore.
	//
	// The keys are application-defined aggregate handler keys. The values are
	// buckets further split into separate buckets for revisions and snapshots.
	aggregateStoreBucketKey = []byte("aggregatestore")

	// aggregateStoreRevisionsBucketKey is the key for a child bucket that
	// contains the current revision of each aggregate instance.
	//
	// The keys are application-defined instance IDs. The values are the
	// instance revisions encoded 8-byte big-endian packets.
	aggregateStoreRevisionsBucketKey = []byte("revisions")

	// aggregateStoreSnapshotsBucketKey is the key for a child bucket that
	// contains snapshots of each aggregate instance.
	//
	// TODO: https://github.com/dogmatiq/infix/issues/142
	// Implement aggregate snapshots.
	aggregateStoreSnapshotsBucketKey = []byte("snapshots")
)

// IncrementAggregateRevision increments the persisted revision of a an
// aggregate instance.
//
// ak is the aggregate handler's identity key, id is the instance ID.
//
// c must be the instance's current revision as persisted, otherwise an
// optimistic concurrency conflict has occurred, the revision is not saved
// and ErrConflict is returned.
func (t *transaction) IncrementAggregateRevision(
	ctx context.Context,
	hk string,
	id string,
	c aggregatestore.Revision,
) (err error) {
	defer bboltx.Recover(&err)

	if err := t.begin(ctx); err != nil {
		return err
	}

	return errors.New("not implemented")
}

// aggregateStoreRepository is an implementation of aggregatestore.Repository
// that stores aggregate state in a BoltDB database.
type aggregateStoreRepository struct {
	db     *database
	appKey []byte
}

// LoadRevision loads the current revision of an aggregate instance.
//
// ak is the aggregate handler's identity key, id is the instance ID.
func (r *aggregateStoreRepository) LoadRevision(
	ctx context.Context,
	hk, id string,
) (rev aggregatestore.Revision, err error) {
	defer bboltx.Recover(&err)

	r.db.View(
		ctx,
		func(tx *bbolt.Tx) {
			revisions, exists := bboltx.TryBucket(
				tx,
				r.appKey,
				aggregateStoreBucketKey,
				[]byte(hk),
				aggregateStoreRevisionsBucketKey,
			)
			if exists {
				rev = unmarshalAggregateRevision(
					revisions.Get([]byte(id)),
				)
			}
		},
	)

	return rev, nil
}

// marshalAggregateRevision marshals an aggregate revision to its binary
// representation.
func marshalAggregateRevision(rev aggregatestore.Revision) []byte {
	return marshalUint64(uint64(rev))
}

// unmarshalAggregateRevision unmarshals an aggregate revision from its binary
// representation.
func unmarshalAggregateRevision(data []byte) aggregatestore.Revision {
	return aggregatestore.Revision(unmarshalUint64(data))
}
