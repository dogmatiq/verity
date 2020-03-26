// +build cgo

package sql_test

import (
	"context"
	"database/sql"

	"github.com/dogmatiq/infix/internal/testing/sqltest"
	"github.com/dogmatiq/infix/persistence/provider/internal/providertest"
	. "github.com/dogmatiq/infix/persistence/provider/sql"
	"github.com/dogmatiq/infix/persistence/provider/sql/sqlite"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("type Provider", func() {
	var db *sql.DB

	providertest.Declare(
		func(ctx context.Context, in providertest.In) providertest.Out {
			db = sqltest.Open("sqlite3")

			err := sqlite.DropSchema(ctx, db)
			Expect(err).ShouldNot(HaveOccurred())

			err = sqlite.CreateSchema(ctx, db)
			Expect(err).ShouldNot(HaveOccurred())

			return providertest.Out{
				Provider: &Provider{
					DB: db,
				},
			}
		},
		func() {
			if db != nil {
				db.Close()
			}
		},
	)
})

var _ = Describe("type DSNProvider", func() {
	var db *sql.DB

	providertest.Declare(
		func(ctx context.Context, in providertest.In) providertest.Out {
			dsn := sqltest.DSN("sqlite3")

			var err error
			db, err = sql.Open("sqlite3", dsn)
			Expect(err).ShouldNot(HaveOccurred())

			err = sqlite.DropSchema(ctx, db)
			Expect(err).ShouldNot(HaveOccurred())

			err = sqlite.CreateSchema(ctx, db)
			Expect(err).ShouldNot(HaveOccurred())

			return providertest.Out{
				Provider: &DSNProvider{
					DriverName: "sqlite3",
					DSN:        dsn,
				},
			}
		},
		func() {
			if db != nil {
				db.Close()
			}
		},
	)
})

var _ = Describe("type DSNProvider", func() {
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
