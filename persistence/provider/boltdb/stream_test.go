package boltdb_test

import (
	"context"

	"github.com/dogmatiq/configkit/message"
	. "github.com/dogmatiq/dogma/fixtures"
	"github.com/dogmatiq/infix/envelope"
	. "github.com/dogmatiq/infix/fixtures"
	"github.com/dogmatiq/infix/internal/testing/boltdbtest"
	"github.com/dogmatiq/infix/internal/testing/streamtest"
	. "github.com/dogmatiq/infix/persistence/provider/boltdb"
	. "github.com/dogmatiq/marshalkit/fixtures"
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
		func(ctx context.Context, in streamtest.In) streamtest.Out {
			db, close = boltdbtest.Open()

			stream := &Stream{
				DB:        db,
				Types:     in.MessageTypes,
				Marshaler: in.Marshaler,
				BucketPath: [][]byte{
					[]byte("path"),
					[]byte("to"),
					[]byte("bucket"),
				},
			}

			return streamtest.Out{
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

var _ = Describe("type Stream", func() {
	Describe("func Append()", func() {
		It("panics if the message type is not supported", func() {
			db, close := boltdbtest.Open()
			defer close()

			env := NewEnvelope("<id>", MessageA1)

			stream := &Stream{
				DB:        db,
				Marshaler: Marshaler,
				Types: message.TypesOf(
					"<not a message type>",
				),
				BucketPath: [][]byte{
					[]byte("path"),
					[]byte("to"),
					[]byte("bucket"),
				},
			}

			Expect(func() {
				db.Update(func(tx *bbolt.Tx) error {
					_, err := stream.Append(tx, env)
					return err
				})
			}).To(Panic())
		})
	})
})
