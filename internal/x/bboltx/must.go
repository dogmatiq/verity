package bboltx

import "go.etcd.io/bbolt"

// Recover recovers from a panic caused by one of the MustXXX() functions.
//
// It is intended to be used in a defer statement. The error that caused the
// panic is assigned to *err.
func Recover(err *error) {
	if err == nil {
		panic("err must be a non-nil pointer")
	}

	switch v := recover().(type) {
	case PanicSentinel:
		*err = v.Cause
	case nil:
		return
	default:
		panic(v)
	}
}

// PanicSentinel is a wrapper value used to identify panic's that are caused
// by one of the MustXXX() functions.
type PanicSentinel struct {
	// Cause is the error that caused the panic.
	Cause error
}

// MustCreateBucketIfNotExists creates nested buckets with names given by the elements of path.
func MustCreateBucketIfNotExists(p BucketParent, path ...[]byte) *bbolt.Bucket {
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
			panic(PanicSentinel{err})
		}

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

// MustPut writes a value to a bucket.
func MustPut(b *bbolt.Bucket, k, v []byte) {
	err := b.Put(k, v)
	if err != nil {
		panic(PanicSentinel{err})
	}
}

// MustBeginRead starts a read-only transaction.
func MustBeginRead(db *bbolt.DB) *bbolt.Tx {
	tx, err := db.Begin(false)
	if err != nil {
		panic(PanicSentinel{err})
	}

	return tx
}

// MustBeginWrite starts a read-only transaction.
func MustBeginWrite(db *bbolt.DB) *bbolt.Tx {
	tx, err := db.Begin(true)
	if err != nil {
		panic(PanicSentinel{err})
	}

	return tx
}
