package boltdb_test

import (
	"context"
	"time"

	"github.com/dogmatiq/configkit"
	"github.com/dogmatiq/configkit/message"
	"github.com/dogmatiq/dogma"
	. "github.com/dogmatiq/dogma/fixtures"
	"github.com/dogmatiq/infix/fixtures" // can't dot-import due to conflicts
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
		ctx     context.Context
		cancel  context.CancelFunc
		db      *bbolt.DB
		close   func()
		cfg     configkit.RichApplication
		entries []TableEntry
	)

	entries = []TableEntry{
		Entry(
			"type Provider",
			func() persistence.Provider {
				return &Provider{db}
			},
		),
		Entry(
			"type FileProvider",
			func() persistence.Provider {
				filename := db.Path()
				db.Close()

				return &FileProvider{
					Path: filename,
				}
			},
		),
	}

	BeforeEach(func() {
		ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)

		db, close = boltdbtest.Open()

		cfg = configkit.FromApplication(&Application{
			ConfigureFunc: func(c dogma.ApplicationConfigurer) {
				c.Identity("<app-name>", "<app-key>")
			},
		})
	})

	AfterEach(func() {
		close()
		cancel()
	})

	DescribeTable(
		"it operates on the expected database",
		func(get func() persistence.Provider) {
			// First we create a stream and write a message.
			env := fixtures.NewEnvelope("<id>", MessageA1)

			writer := &Stream{
				App:       configkit.MustNewIdentity("<app-name>", "<app-key>"),
				DB:        db,
				Types:     message.TypesOf(MessageA1),
				Marshaler: Marshaler,
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
				cfg,
				Marshaler,
			)
			Expect(err).ShouldNot(HaveOccurred())
			defer store.Close()

			reader := store.EventStream()

			cur, err := reader.Open(ctx, 0, message.TypesOf(MessageA1))
			Expect(err).ShouldNot(HaveOccurred())
			defer cur.Close()

			ev, err := cur.Next(ctx)
			Expect(err).ShouldNot(HaveOccurred())
			Expect(ev.Envelope).To(Equal(env))
		},
		entries...,
	)

	DescribeTable(
		"it allows multiple open data-stores for the same application",
		func(get func() persistence.Provider) {
			provider := get()

			store1, err := provider.Open(
				ctx,
				cfg,
				Marshaler,
			)
			Expect(err).ShouldNot(HaveOccurred())
			defer store1.Close()

			store2, err := provider.Open(
				ctx,
				cfg,
				Marshaler,
			)
			Expect(err).ShouldNot(HaveOccurred())
			defer store2.Close()
		},
		entries...,
	)

	Describe("type FileProvider", func() {
		Describe("func Open()", func() {
			It("returns an error if the DB can not be opened", func() {
				ctx, cancel := context.WithTimeout(ctx, 10*time.Millisecond)
				defer cancel()

				p := &FileProvider{
					Path: db.Path(),
				}

				// Note that we did not close db, so we expect this to timeout
				// waiting for the file lock.
				_, err := p.Open(
					ctx,
					cfg,
					Marshaler,
				)
				Expect(err).To(Equal(context.DeadlineExceeded))
			})
		})
	})
})
