package boltpersistence

import (
	"context"

	"github.com/dogmatiq/verity/internal/x/bboltx"
	"github.com/dogmatiq/verity/persistence"
	"github.com/dogmatiq/verity/persistence/boltpersistence/internal/pb"
	"go.etcd.io/bbolt"
	"google.golang.org/protobuf/proto"
)

var (
	// processBucketKey is the key for the root bucket for process data.
	//
	// The keys are application-defined process handler keys. The values are
	// buckets containing instances of the handler.
	//
	// Within this further sub-bucket, the keys are the application-defined
	// process instance IDs. The values are pb.ProcessInstance values marshaled
	// using protocol buffers.
	processBucketKey = []byte("process")
)

// LoadProcessInstance loads a process instance.
//
// hk is the process handler's identity key, id is the instance ID.
func (ds *dataStore) LoadProcessInstance(
	ctx context.Context,
	hk, id string,
) (_ persistence.ProcessInstance, err error) {
	defer bboltx.Recover(&err)

	inst := persistence.ProcessInstance{
		HandlerKey: hk,
		InstanceID: id,
	}

	bboltx.View(
		ds.db,
		func(tx *bbolt.Tx) {
			if root, ok := bboltx.TryBucket(
				tx,
				ds.appKey,
			); ok {
				pb := loadProcessInstance(root, hk, id)
				inst.Revision = pb.GetRevision()
				inst.Packet.MediaType = pb.GetMediaType()
				inst.Packet.Data = pb.GetData()
			}
		},
	)

	return inst, nil

}

// VisitSaveProcessInstance applies the changes in a "SaveProcessInstance"
// operation to the database.
func (c *committer) VisitSaveProcessInstance(
	ctx context.Context,
	op persistence.SaveProcessInstance,
) error {
	existing := loadProcessInstance(
		c.root,
		op.Instance.HandlerKey,
		op.Instance.InstanceID,
	)

	if op.Instance.Revision != existing.GetRevision() {
		return persistence.ConflictError{
			Cause: op,
		}
	}

	saveProcessInstance(c.root, op.Instance)

	return nil
}

// VisitRemoveProcessInstance applies the changes in a "RemoveProcessInstance"
// operation to the database.
func (c *committer) VisitRemoveProcessInstance(
	ctx context.Context,
	op persistence.RemoveProcessInstance,
) error {
	existing := loadProcessInstance(
		c.root,
		op.Instance.HandlerKey,
		op.Instance.InstanceID,
	)

	if existing == nil || op.Instance.Revision != existing.GetRevision() {
		return persistence.ConflictError{
			Cause: op,
		}
	}

	bboltx.DeletePath(
		c.root,
		processBucketKey,
		[]byte(op.Instance.HandlerKey),
		[]byte(op.Instance.InstanceID),
	)

	c.removeTimeoutsByProcessInstance(
		op.Instance.HandlerKey,
		op.Instance.InstanceID,
	)

	return nil
}

// saveProcessInstance saves a process instance to b. inst.Revision is
// incremented before saving.
func saveProcessInstance(root *bbolt.Bucket, inst persistence.ProcessInstance) {
	data, err := proto.Marshal(
		&pb.ProcessInstance{
			Revision:  inst.Revision + 1,
			MediaType: inst.Packet.MediaType,
			Data:      inst.Packet.Data,
		},
	)
	bboltx.Must(err)

	bboltx.PutPath(
		root,
		data,
		processBucketKey,
		[]byte(inst.HandlerKey),
		[]byte(inst.InstanceID),
	)
}

// loadProcessInstance returns a process instance loaded from b.
func loadProcessInstance(root *bbolt.Bucket, hk, id string) *pb.ProcessInstance {
	data := bboltx.GetPath(
		root,
		processBucketKey,
		[]byte(hk),
		[]byte(id),
	)
	if data == nil {
		return nil
	}

	inst := &pb.ProcessInstance{}
	err := proto.Unmarshal(data, inst)
	bboltx.Must(err)

	return inst
}
