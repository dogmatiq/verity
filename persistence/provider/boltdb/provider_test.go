package boltdb_test

import (
	"context"
	"time"

	"github.com/dogmatiq/configkit"
	"github.com/dogmatiq/configkit/message"
	. "github.com/dogmatiq/dogma/fixtures"
	. "github.com/dogmatiq/infix/fixtures"
	"github.com/dogmatiq/infix/internal/testing/boltdbtest"
	"github.com/dogmatiq/infix/persistence"
	. "github.com/dogmatiq/infix/persistence/provider/boltdb"
	. "github.com/dogmatiq/marshalkit/fixtures"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/ginkgo/extensions/table"
	. "github.com/onsi/gomega"
	"go.etcd.io/bbolt"
)

var _ = Context("providers", func() {
	var (
		ctx    context.Context
		cancel context.CancelFunc
		db     *bbolt.DB
		close  func()
	)

	BeforeEach(func() {
		ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
		db, close = boltdbtest.Open()
	})

	AfterEach(func() {
		close()
		cancel()
	})

	DescribeTable(
		"it operates on the expected database",
		func(get func() persistence.Provider) {
			// First we create a stream and write a message.
			env := NewEnvelope("<id>", MessageA1)

			writer := &Stream{
				DB:        db,
				Marshaler: Marshaler,
				BucketPath: [][]byte{
					[]byte("<app-key>"),
					[]byte("eventstream"),
				},
			}

			err := db.Update(func(tx *bbolt.Tx) error {
				_, err := writer.Append(tx, env)
				return err
			})
			Expect(err).ShouldNot(HaveOccurred())

			// Then we create the provider. and confirm that it gives us a
			// data-store that reads from the same database.
			provider := get()

			store, err := provider.Open(
				ctx,
				configkit.MustNewIdentity("<app>", "<app-key>"),
				Marshaler,
			)
			Expect(err).ShouldNot(HaveOccurred())
			defer store.Close()

			reader, err := store.EventStream(ctx)
			Expect(err).ShouldNot(HaveOccurred())

			cur, err := reader.Open(ctx, 0, message.TypesOf(MessageA1))
			Expect(err).ShouldNot(HaveOccurred())
			defer cur.Close()

			m, err := cur.Next(ctx)
			Expect(err).ShouldNot(HaveOccurred())
			Expect(m.Envelope).To(Equal(env))
		},
		Entry(
			"func New()",
			func() persistence.Provider {
				return New(db)
			},
		),
		Entry(
			"func NewOpener()",
			func() persistence.Provider {
				filename := db.Path()
				db.Close()

				return NewOpener(filename, 0, nil)
			},
		),
	)
})
