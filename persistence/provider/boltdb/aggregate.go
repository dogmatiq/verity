package boltdb

import (
	"context"

	"github.com/dogmatiq/verity/internal/x/bboltx"
	"github.com/dogmatiq/verity/persistence"
	"github.com/dogmatiq/verity/persistence/provider/boltdb/internal/pb"
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
// hk is the aggregate handler's identity key, id is the instance ID.
func (ds *dataStore) LoadAggregateMetaData(
	ctx context.Context,
	hk, id string,
) (_ persistence.AggregateMetaData, err error) {
	defer bboltx.Recover(&err)

	md := persistence.AggregateMetaData{
		HandlerKey: hk,
		InstanceID: id,
	}

	bboltx.View(
		ds.db,
		func(tx *bbolt.Tx) {
			if root, ok := bboltx.TryBucket(tx, ds.appKey); ok {
				pb := loadAggregateMetaData(root, hk, id)
				md.Revision = pb.GetRevision()
				md.InstanceExists = pb.GetInstanceExists()
				md.LastEventID = pb.GetLastEventId()
				md.BarrierEventID = pb.GetBarrierEventId()
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
	existing := loadAggregateMetaData(
		c.root,
		op.MetaData.HandlerKey,
		op.MetaData.InstanceID,
	)

	if op.MetaData.Revision != existing.GetRevision() {
		return persistence.ConflictError{
			Cause: op,
		}
	}

	saveAggregateMetaData(c.root, op.MetaData)

	return nil
}

// saveAggregateMetaData saves aggregate meta-data to b. md.Revision is
// incremented before saving.
func saveAggregateMetaData(root *bbolt.Bucket, md persistence.AggregateMetaData) {
	data, err := proto.Marshal(
		&pb.AggregateMetaData{
			Revision:       md.Revision + 1,
			InstanceExists: md.InstanceExists,
			LastEventId:    md.LastEventID,
			BarrierEventId: md.BarrierEventID,
		},
	)
	bboltx.Must(err)

	bboltx.PutPath(
		root,
		data,
		aggregateBucketKey,
		[]byte(md.HandlerKey),
		aggregateMetaDataBucketKey,
		[]byte(md.InstanceID),
	)
}

// loadAggregateMetaData returns aggregate meta-data for a specific instance.
func loadAggregateMetaData(root *bbolt.Bucket, hk, id string) *pb.AggregateMetaData {
	data := bboltx.GetPath(
		root,
		aggregateBucketKey,
		[]byte(hk),
		aggregateMetaDataBucketKey,
		[]byte(id),
	)
	if data == nil {
		return nil
	}

	md := &pb.AggregateMetaData{}
	err := proto.Unmarshal(data, md)
	bboltx.Must(err)

	return md
}
