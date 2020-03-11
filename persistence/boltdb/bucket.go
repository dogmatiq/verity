package boltdb

import "go.etcd.io/bbolt"

var (
	_ bucketParent = (*bbolt.Tx)(nil)
	_ bucketParent = (*bbolt.Bucket)(nil)
)

func dbRecover(err *error) {
	switch p := recover().(type) {
	case panicSentinel:
		*err = p.Error
	case nil:
		return
	default:
		panic(p)
	}
}

func dbPanic(err error) {
	panic(panicSentinel{err})
}

type panicSentinel struct {
	Error error
}

// bucketParent is an interface for things that contain buckets.
type bucketParent interface {
	CreateBucketIfNotExists([]byte) (*bbolt.Bucket, error)
	Bucket([]byte) *bbolt.Bucket
}

// createBucket creates nested buckets with names given by the elements of path.
func createBucket(p bucketParent, path ...[]byte) *bbolt.Bucket {
	if len(path) == 0 {
		panic("at least one path element must be provided")
	}

	var (
		b   *bbolt.Bucket
		err error
	)

	for _, n := range path {
		b, err = p.CreateBucketIfNotExists(n)
		if err != nil {
			dbPanic(err)
		}

		p = b
	}

	return b
}

// bucket gets nested buckets with names given by the elements of path.
func bucket(p bucketParent, path ...[]byte) (b *bbolt.Bucket, ok bool) {
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

// put writes a value to a bucket.
func put(b *bbolt.Bucket, k, v []byte) {
	err := b.Put(k, v)
	if err != nil {
		dbPanic(err)
	}
}

// beginRead starts a read-only transaction.
func beginRead(db *bbolt.DB) *bbolt.Tx {
	tx, err := db.Begin(false)
	if err != nil {
		dbPanic(err)
	}
	return tx
}

// beginWrite starts a read-only transaction.
func beginWrite(db *bbolt.DB) *bbolt.Tx {
	tx, err := db.Begin(true)
	if err != nil {
		dbPanic(err)
	}
	return tx
}
