package bboltx

import "go.etcd.io/bbolt"

// CreateBucketIfNotExists creates nested buckets with names given by the elements of path.
func CreateBucketIfNotExists(p BucketParent, path ...[]byte) *bbolt.Bucket {
	if len(path) == 0 {
		panic("at least one path element must be provided")
	}

	var (
		b   *bbolt.Bucket
		err error
	)

	for _, n := range path {
		b, err = p.CreateBucketIfNotExists(n)
		Must(err)

		p = b
	}

	return b
}

// Bucket gets nested buckets with names given by the elements of path.
//
// It returns nil if any of the nested buckets does not exist.
func Bucket(p BucketParent, path ...[]byte) (b *bbolt.Bucket) {
	if len(path) == 0 {
		panic("at least one path element must be provided")
	}

	for _, n := range path {
		b = p.Bucket(n)
		if b == nil {
			return nil
		}

		p = b
	}

	return b
}

// Put writes a value to a bucket.
func Put(b *bbolt.Bucket, k, v []byte) {
	err := b.Put(k, v)
	Must(err)
}
