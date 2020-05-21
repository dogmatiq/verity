package boltdb

import (
	"context"

	"github.com/dogmatiq/infix/internal/x/bboltx"
	"github.com/dogmatiq/infix/persistence"
	"github.com/dogmatiq/infix/persistence/provider/boltdb/internal/pb"
	"github.com/golang/protobuf/proto"
	"go.etcd.io/bbolt"
)

var (
	// aggregateBucketKey is the key for the root bucket for aggregate data.
	//
	// The keys are application-defined aggregate handler keys. The values are
	// buckets further split into separate buckets for meta-data and snapshots.
	aggregateBucketKey = []byte("aggregate")

	// aggregateMetaDataBucketKey is the key for a child bucket that contains
	// the meta-data for each aggregate instance of a specific handler type.
	//
	// The keys are application-defined instance IDs. The values are
	// pb.AggregateMetaData values marshaled using protocol buffers.
	aggregateMetaDataBucketKey = []byte("metadata")
)

// LoadAggregateMetaData loads the meta-data for an aggregate instance.
//
// ak is the aggregate handler's identity key, id is the instance ID.
func (ds *dataStore) LoadAggregateMetaData(
	ctx context.Context,
	hk, id string,
) (_ *persistence.AggregateMetaData, err error) {
	defer bboltx.Recover(&err)

	md := &persistence.AggregateMetaData{
		HandlerKey: hk,
		InstanceID: id,
	}

	bboltx.View(
		ds.db,
		func(tx *bbolt.Tx) {
			if metadata, ok := bboltx.TryBucket(
				tx,
				ds.appKey,
				aggregateBucketKey,
				[]byte(hk),
				aggregateMetaDataBucketKey,
			); ok {
				pb := loadAggregateMetaData(metadata, id)
				md.Revision = pb.GetRevision()
				md.InstanceExists = pb.GetInstanceExists()
				md.LastDestroyedBy = pb.GetLastDestroyedBy()
			}
		},
	)

	return md, nil
}

// VisitSaveAggregateMetaData applies the changes in a "SaveAggregateMetaData"
// operation to the database.
func (c *committer) VisitSaveAggregateMetaData(
	_ context.Context,
	op persistence.SaveAggregateMetaData,
) error {
	metadata := bboltx.CreateBucketIfNotExists(
		c.root,
		aggregateBucketKey,
		[]byte(op.MetaData.HandlerKey),
		aggregateMetaDataBucketKey,
	)

	existing := loadAggregateMetaData(metadata, op.MetaData.InstanceID)

	if op.MetaData.Revision != existing.GetRevision() {
		return persistence.ConflictError{
			Cause: op,
		}
	}

	saveAggregateMetaData(metadata, op.MetaData)

	return nil
}

// saveAggregateMetaData saves aggregate meta-data to b. md.Revision is
// incremented before saving.
func saveAggregateMetaData(b *bbolt.Bucket, md persistence.AggregateMetaData) {
	data, err := proto.Marshal(
		&pb.AggregateMetaData{
			Revision:        md.Revision + 1,
			InstanceExists:  md.InstanceExists,
			LastDestroyedBy: md.LastDestroyedBy,
		},
	)
	bboltx.Must(err)
	bboltx.Put(b, []byte(md.InstanceID), data)
}

// loadAggregateMetaData returns aggregate meta-data loaded from b.
func loadAggregateMetaData(b *bbolt.Bucket, id string) *pb.AggregateMetaData {
	data := b.Get([]byte(id))
	if data == nil {
		return nil
	}

	md := &pb.AggregateMetaData{}
	err := proto.Unmarshal(data, md)
	bboltx.Must(err)

	return md
}
