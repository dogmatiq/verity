package boltdb

import "go.etcd.io/bbolt"

var (
	_ bucketParent = (*bbolt.Tx)(nil)
	_ bucketParent = (*bbolt.Bucket)(nil)
)

// bucketParent is an interface for things that contain buckets.
type bucketParent interface {
	CreateBucketIfNotExists([]byte) (*bbolt.Bucket, error)
	Bucket([]byte) *bbolt.Bucket
}

// createBucket creates nested buckets with names given by the elements of path.
func createBucket(p bucketParent, path ...[]byte) (b *bbolt.Bucket, err error) {
	if len(path) == 0 {
		panic("at least one path element must be provided")
	}

	for _, n := range path {
		b, err = p.CreateBucketIfNotExists(n)
		if err != nil {
			return nil, err
		}

		p = b
	}

	return b, nil
}

// getBucket gets nested buckets with names given by the elements of path.
func getBucket(p bucketParent, path ...[]byte) (b *bbolt.Bucket, ok bool) {
	if len(path) == 0 {
		panic("at least one path element must be provided")
	}

	for _, n := range path {
		b = p.Bucket(n)
		if b == nil {
			return nil, false
		}

		p = b
	}

	return b, true
}
