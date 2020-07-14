package bboltx

import (
	"bytes"
	"fmt"

	"go.etcd.io/bbolt"
)

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

// TryBucket gets nested buckets with names given by the elements of path.
func TryBucket(p BucketParent, path ...[]byte) (b *bbolt.Bucket, ok bool) {
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

// Bucket gets nested buckets with names given by the elements of path.
//
// It panics if any of the nested buckets does not exist.
func Bucket(p BucketParent, path ...[]byte) *bbolt.Bucket {
	if b, ok := TryBucket(p, path...); ok {
		return b
	}

	err := fmt.Errorf(
		"bucket '%s' does not exist",
		bytes.Join(path, []byte(".")),
	)
	panic(PanicSentinel{err})
}

// GetPath loads a value at a given path. It returns nil if any of the
// intermediate buckets do not exist.
//
// The last element in the path is the value's key. All preceeding elements are
// bucket names. Therefore, the path must contain at least 2 elements (1 bucket,
// and the key).
func GetPath(p BucketParent, path ...[]byte) []byte {
	k, path := splitKey(path)
	b, ok := TryBucket(p, path...)
	if !ok {
		return nil
	}

	return b.Get(k)
}

// PutPath stores a value at a given path, creating intermediate buckets as
// necessary.
//
// The last element in the path is the value's key. All preceeding elements are
// bucket names. Therefore, the path must contain at least 2 elements (1 bucket,
// and the key).
func PutPath(p BucketParent, v []byte, path ...[]byte) {
	k, path := splitKey(path)
	b := CreateBucketIfNotExists(p, path...)
	Must(b.Put(k, v))
}

// DeletePath removes a value at a given path, recursively deleting any
// intermediate buckets that become empty.
//
// The last element in the path is the value's key. All preceeding elements are
// bucket names. Therefore, the path must contain at least 2 elements (1 bucket,
// and the key).
func DeletePath(p BucketParent, path ...[]byte) {
	k, path := splitKey(path)

	var (
		parents []BucketParent
		buckets []*bbolt.Bucket
	)

	for _, n := range path {
		b := p.Bucket(n)
		if b == nil {
			break
		}

		parents = append(parents, p)
		buckets = append(buckets, b)
		p = b
	}

	count := len(buckets)
	if count == len(path) {
		Must(
			buckets[count-1].Delete(k),
		)
	}

	for i := count - 1; i >= 0; i-- {
		b := buckets[i]

		if !isEmpty(b) {
			return
		}

		Must(
			parents[i].DeleteBucket(path[i]),
		)
	}
}

// splitKey splits a value's key from the path.
// It panics if too-few path elements are provided.
func splitKey(path [][]byte) ([]byte, [][]byte) {
	n := len(path)
	if n < 2 {
		panic("at least one bucket name and key must be specified")
	}

	return path[n-1], path[:n-1]
}

// isEmpty returns true if b contains no keys or buckets.
func isEmpty(b *bbolt.Bucket) bool {
	k, _ := b.Cursor().First()
	return k == nil
}
