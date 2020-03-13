package boltdb_test

import (
	"context"

	"github.com/dogmatiq/infix/envelope"
	"github.com/dogmatiq/infix/internal/testing/boltdbtest"
	"github.com/dogmatiq/infix/persistence/internal/streamtest"
	. "github.com/dogmatiq/infix/persistence/provider/boltdb"
	"github.com/dogmatiq/marshalkit"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"go.etcd.io/bbolt"
)

var _ = Describe("type Stream (standard test suite)", func() {
	var (
		db    *bbolt.DB
		close func()
	)

	streamtest.Declare(
		func(ctx context.Context, m marshalkit.Marshaler) streamtest.Config {
			db, close = boltdbtest.Open()

			stream := &Stream{
				DB:        db,
				Marshaler: m,
				BucketPath: [][]byte{
					[]byte("path"),
					[]byte("to"),
					[]byte("bucket"),
				},
			}

			return streamtest.Config{
				Stream: stream,
				Append: func(ctx context.Context, envelopes ...*envelope.Envelope) {
					err := db.Update(func(tx *bbolt.Tx) error {
						_, err := stream.Append(tx, envelopes...)
						return err
					})
					Expect(err).ShouldNot(HaveOccurred())
				},
			}
		},
		func() {
			close()
		},
	)
})
