// +build cgo

package sql_test

import (
	"context"
	"database/sql"

	"github.com/dogmatiq/infix/internal/testing/sqltest"
	"github.com/dogmatiq/infix/persistence"
	"github.com/dogmatiq/infix/persistence/provider/internal/providertest"
	. "github.com/dogmatiq/infix/persistence/provider/sql"
	"github.com/dogmatiq/infix/persistence/provider/sql/sqlite"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("type Provider", func() {
	var (
		db    *sql.DB
		close func()
	)

	providertest.Declare(
		func(ctx context.Context, in providertest.In) providertest.Out {
			db, _, close = sqltest.Open("sqlite3")

			err := sqlite.DropSchema(ctx, db)
			Expect(err).ShouldNot(HaveOccurred())

			err = sqlite.CreateSchema(ctx, db)
			Expect(err).ShouldNot(HaveOccurred())

			return providertest.Out{
				NewProvider: func() (persistence.Provider, func()) {
					return &Provider{
						DB: db,
					}, nil
				},
				IsShared: true,
			}
		},
		func() {
			if close != nil {
				close()
			}
		},
	)
})

var _ = Describe("type DSNProvider", func() {
	var (
		dsn   string
		db    *sql.DB
		close func()
	)

	providertest.Declare(
		func(ctx context.Context, in providertest.In) providertest.Out {
			db, dsn, close = sqltest.Open("sqlite3")

			// This SQLite DB is held open for the lifetime of the test to
			// keep the schema in memory.
			var err error
			db, err = sql.Open("sqlite3", dsn)
			Expect(err).ShouldNot(HaveOccurred())

			err = sqlite.DropSchema(ctx, db)
			Expect(err).ShouldNot(HaveOccurred())

			err = sqlite.CreateSchema(ctx, db)
			Expect(err).ShouldNot(HaveOccurred())

			return providertest.Out{
				NewProvider: func() (persistence.Provider, func()) {
					return &DSNProvider{
						DriverName: "sqlite3",
						DSN:        dsn,
					}, nil
				},
				IsShared: true,
			}
		},
		func() {
			if close != nil {
				close()
			}
		},
	)

	Describe("func Open()", func() {
		It("returns an error if the DB can not be opened", func() {
			provider := &DSNProvider{
				DriverName: "<nonsense-driver>",
				DSN:        "<nonsense-dsn>",
			}

			ds, err := provider.Open(context.Background(), "<app-key>")
			if ds != nil {
				ds.Close()
			}
			Expect(err).Should(HaveOccurred())
		})
	})
})

var _ = Describe("type provider", func() {
	Describe("func open()", func() {
		It("returns an error if the driver can not be deduced", func() {
			provider := &Provider{
				DB: sqltest.MockDB(),
			}

			ds, err := provider.Open(context.Background(), "<app-key>")
			if ds != nil {
				ds.Close()
			}
			Expect(err).To(MatchError("can not deduce the appropriate SQL driver for *sqltest.MockDriver"))
		})
	})
})
