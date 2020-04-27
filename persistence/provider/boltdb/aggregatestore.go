package boltdb

import (
	"context"

	"github.com/dogmatiq/infix/internal/x/bboltx"
	"github.com/dogmatiq/infix/persistence/provider/boltdb/internal/pb"
	"github.com/dogmatiq/infix/persistence/subsystem/aggregatestore"
	"github.com/golang/protobuf/proto"
	"go.etcd.io/bbolt"
)

var (
	// aggregateStoreBucketKey is the key for the bucket at the root of the
	// aggregatestore.
	//
	// The keys are application-defined aggregate handler keys. The values are
	// buckets further split into separate buckets for revisions and snapshots.
	aggregateStoreBucketKey = []byte("aggregatestore")

	// aggregateStoreMetaDataBucketKey is the key for a child bucket that
	// contains the meta-data for each aggregate instance.
	//
	// The keys are application-defined instance IDs. The values are
	// pb.AggregateStoreMetaData values marshaled using protocol buffers.
	aggregateStoreMetaDataBucketKey = []byte("metadata")

	// aggregateStoreSnapshotsBucketKey is the key for a child bucket that
	// contains snapshots of each aggregate instance.
	//
	// TODO: https://github.com/dogmatiq/infix/issues/142
	// Implement aggregate snapshots.
	aggregateStoreSnapshotsBucketKey = []byte("snapshots")
)

// SaveAggregateMetaData persists meta-data about an aggregate instance.
//
// md.Revision must be the revision of the instance as currently persisted,
// otherwise an optimistic concurrency conflict has occurred, the meta-data is
// not saved and ErrConflict is returned.
func (t *transaction) SaveAggregateMetaData(
	ctx context.Context,
	md *aggregatestore.MetaData,
) (err error) {
	defer bboltx.Recover(&err)

	if err := t.begin(ctx); err != nil {
		return err
	}

	metadata := bboltx.CreateBucketIfNotExists(
		t.actual,
		t.appKey,
		aggregateStoreBucketKey,
		[]byte(md.HandlerKey),
		aggregateStoreMetaDataBucketKey,
	)

	old := loadAggregateStoreMetaData(metadata, md.InstanceID)

	if uint64(md.Revision) != old.GetRevision() {
		return aggregatestore.ErrConflict
	}

	data := marshalAggregateStoreMetaData(md)
	bboltx.Put(metadata, []byte(md.InstanceID), data)

	return nil
}

// aggregateStoreRepository is an implementation of aggregatestore.Repository
// that stores aggregate state in a BoltDB database.
type aggregateStoreRepository struct {
	db     *database
	appKey []byte
}

// LoadMetaData loads the meta-data for an aggregate instance.
//
// ak is the aggregate handler's identity key, id is the instance ID.
func (r *aggregateStoreRepository) LoadMetaData(
	ctx context.Context,
	hk, id string,
) (_ *aggregatestore.MetaData, err error) {
	defer bboltx.Recover(&err)

	md := &aggregatestore.MetaData{
		HandlerKey: hk,
		InstanceID: id,
	}

	r.db.View(
		ctx,
		func(tx *bbolt.Tx) {
			metadata, exists := bboltx.TryBucket(
				tx,
				r.appKey,
				aggregateStoreBucketKey,
				[]byte(hk),
				aggregateStoreMetaDataBucketKey,
			)

			if exists {
				pb := loadAggregateStoreMetaData(metadata, id)
				md.Revision = pb.GetRevision()
				md.BeginOffset = pb.GetBeginOffset()
				md.EndOffset = pb.GetEndOffset()
			}
		},
	)

	return md, nil
}

// marshalAggregateStoreMetaData marshals an aggregatestore.MetaData to its binary
// representation with an incremented revision.
func marshalAggregateStoreMetaData(md *aggregatestore.MetaData) []byte {
	new := &pb.AggregateStoreMetaData{
		Revision:    uint64(md.Revision + 1),
		BeginOffset: md.BeginOffset,
		EndOffset:   md.EndOffset,
	}

	data, err := proto.Marshal(new)
	bboltx.Must(err)

	return data
}

// loadAggregateStoreMetaData loads meta-data from the aggregate store.
func loadAggregateStoreMetaData(metadata *bbolt.Bucket, id string) *pb.AggregateStoreMetaData {
	data := metadata.Get([]byte(id))
	if data == nil {
		return nil
	}

	md := &pb.AggregateStoreMetaData{}
	err := proto.Unmarshal(data, md)
	bboltx.Must(err)

	return md
}
