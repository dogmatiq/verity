package bboltx

import "go.etcd.io/bbolt"

var (
	_ BucketParent = (*bbolt.Tx)(nil)
	_ BucketParent = (*bbolt.Bucket)(nil)
)

// BucketParent is an interface for things that contain buckets.
type BucketParent interface {
	CreateBucketIfNotExists([]byte) (*bbolt.Bucket, error)
	Bucket([]byte) *bbolt.Bucket
}
