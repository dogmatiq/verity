package boltdb

import (
	"context"

	"github.com/dogmatiq/infix/internal/x/bboltx"
	"github.com/dogmatiq/infix/persistence"
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
	ctx context.Context,
	ak string,
) (_ uint64, err error) {
	var offset uint64

	bboltx.View(
		ds.db,
		func(tx *bbolt.Tx) {
			if offsets, ok := bboltx.TryBucket(
				tx,
				ds.appKey,
				offsetBucketKey,
			); ok {
				offset = unmarshalUint64(
					offsets.Get([]byte(ak)),
				)
			}
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
	offsets := bboltx.CreateBucketIfNotExists(
		c.root,
		offsetBucketKey,
	)

	current := unmarshalUint64(
		offsets.Get([]byte(op.ApplicationKey)),
	)

	if op.CurrentOffset != current {
		return persistence.ConflictError{
			Cause: op,
		}
	}

	bboltx.Put(
		offsets,
		[]byte(op.ApplicationKey),
		marshalUint64(op.NextOffset),
	)

	return nil
}
