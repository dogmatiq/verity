package boltpersistence

import (
	"context"

	"github.com/dogmatiq/verity/internal/x/bboltx"
	"github.com/dogmatiq/verity/persistence"
	"go.etcd.io/bbolt"
)

var (
	// offsetBucketKey is the key for the bucket used to store offsets.
	//
	// The keys are the event offsets encoded as 8-byte big-endian packets. The
	// values are envelopespec. Envelope values marshaled using protocol
	// buffers.
	offsetBucketKey = []byte("offset")
)

// LoadOffset loads the offset associated with a specific application.
func (ds *dataStore) LoadOffset(
	_ context.Context,
	ak string,
) (_ uint64, err error) {
	var offset uint64

	bboltx.View(
		ds.db,
		func(tx *bbolt.Tx) {
			offset = unmarshalUint64(
				bboltx.GetPath(
					tx,
					ds.appKey,
					offsetBucketKey,
					[]byte(ak),
				),
			)
		},
	)

	return offset, nil
}

// VisitSaveOffset applies the changes in a "SaveOffset" operation to the
// database.
func (c *committer) VisitSaveOffset(
	_ context.Context,
	op persistence.SaveOffset,
) error {
	current := unmarshalUint64(
		bboltx.GetPath(
			c.root,
			offsetBucketKey,
			[]byte(op.ApplicationKey),
		),
	)

	if op.CurrentOffset != current {
		return persistence.ConflictError{
			Cause: op,
		}
	}

	bboltx.PutPath(
		c.root,
		marshalUint64(op.NextOffset),
		offsetBucketKey,
		[]byte(op.ApplicationKey),
	)

	return nil
}
