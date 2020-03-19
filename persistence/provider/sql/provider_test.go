// +build cgo

package sql_test

import (
	"context"
	"database/sql"
	"time"

	"github.com/dogmatiq/configkit"
	"github.com/dogmatiq/configkit/message"
	"github.com/dogmatiq/dogma"
	. "github.com/dogmatiq/dogma/fixtures"
	"github.com/dogmatiq/infix/fixtures" // can't dot-import due to conflicts
	"github.com/dogmatiq/infix/internal/testing/sqltest"
	"github.com/dogmatiq/infix/internal/x/sqlx"
	"github.com/dogmatiq/infix/persistence"
	. "github.com/dogmatiq/infix/persistence/provider/sql"
	"github.com/dogmatiq/infix/persistence/provider/sql/driver/sqlite"
	. "github.com/dogmatiq/marshalkit/fixtures"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/ginkgo/extensions/table"
	. "github.com/onsi/gomega"
)

var _ = Context("providers", func() {
	var (
		ctx     context.Context
		cancel  context.CancelFunc
		dsn     string
		db      *sql.DB
		cfg     configkit.RichApplication
		entries []TableEntry
	)

	entries = []TableEntry{
		Entry(
			"type Provider",
			func() persistence.Provider {
				return &Provider{
					DB: db,
				}
			},
		),
		Entry(
			"type DSNProvider",
			func() persistence.Provider {
				return &DSNProvider{
					DriverName: "sqlite3",
					DSN:        dsn,
				}
			},
		),
	}

	BeforeEach(func() {
		ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)

		dsn = sqltest.DSN("sqlite3")

		var err error
		db, err = sql.Open("sqlite3", dsn)
		Expect(err).ShouldNot(HaveOccurred())

		err = sqlite.DropSchema(ctx, db)
		Expect(err).ShouldNot(HaveOccurred())

		err = sqlite.CreateSchema(ctx, db)
		Expect(err).ShouldNot(HaveOccurred())

		cfg = configkit.FromApplication(&Application{
			ConfigureFunc: func(c dogma.ApplicationConfigurer) {
				c.Identity("<app>", "<app-key>")
			},
		})
	})

	AfterEach(func() {
		if db != nil {
			db.Close()
		}

		cancel()
	})

	DescribeTable(
		"it operates on the expected database",
		func(get func() persistence.Provider) {
			// First we create a stream and write a message.
			env := fixtures.NewEnvelope("<id>", MessageA1)

			writer := &Stream{
				AppKey:    "<app-key>",
				DB:        db,
				Types:     message.TypesOf(MessageA1),
				Driver:    sqlite.StreamDriver{},
				Marshaler: Marshaler,
			}

			tx := sqlx.Begin(ctx, db)
			defer tx.Rollback()

			_, err := writer.Append(ctx, tx, env)
			Expect(err).ShouldNot(HaveOccurred())

			sqlx.Commit(tx)

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

			reader, err := store.EventStream(ctx)
			Expect(err).ShouldNot(HaveOccurred())

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

	Describe("type Provider", func() {
		Describe("func Open()", func() {
			It("returns an error if the driver can not be deduced", func() {
				p := &Provider{
					DB: sqltest.MockDB(),
				}

				_, err := p.Open(
					ctx,
					cfg,
					Marshaler,
				)
				Expect(err).To(MatchError("can not deduce the appropriate SQL driver for *sqltest.MockDriver"))
			})
		})
	})

	Describe("type FileProvider", func() {
		Describe("func Open()", func() {
			It("returns an error if the DB can not be opened", func() {
				p := &DSNProvider{
					DriverName: "<nonsense-driver>",
					DSN:        "<nonsense-dsn>",
				}

				_, err := p.Open(
					ctx,
					cfg,
					Marshaler,
				)
				Expect(err).Should(HaveOccurred())
			})

			It("returns an error if the driver can not be deduced", func() {
				p := &DSNProvider{
					DriverName: sqltest.MockDriverName(),
					DSN:        "<nonsense-dsn>",
				}

				_, err := p.Open(
					ctx,
					cfg,
					Marshaler,
				)
				Expect(err).To(MatchError("can not deduce the appropriate SQL driver for *sqltest.MockDriver"))
			})
		})
	})
})
