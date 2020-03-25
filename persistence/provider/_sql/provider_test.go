// +build cgo

package sql_test

import (
	"context"
	"database/sql"

	"github.com/dogmatiq/configkit"
	"github.com/dogmatiq/dogma"
	. "github.com/dogmatiq/dogma/fixtures" // can't dot-import due to conflicts
	"github.com/dogmatiq/infix/internal/testing/sqltest"
	"github.com/dogmatiq/infix/persistence/internal/providertest"
	. "github.com/dogmatiq/infix/persistence/provider/sql"
	"github.com/dogmatiq/infix/persistence/provider/sql/driver/sqlite"
	. "github.com/dogmatiq/marshalkit/fixtures"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("type Provider", func() {
	var (
		db       *sql.DB
		provider *Provider
	)

	providertest.Declare(
		func(ctx context.Context, in providertest.In) providertest.Out {
			db = sqltest.Open("sqlite3")

			err := sqlite.DropSchema(ctx, db)
			Expect(err).ShouldNot(HaveOccurred())

			err = sqlite.CreateSchema(ctx, db)
			Expect(err).ShouldNot(HaveOccurred())

			provider = &Provider{
				DB: db,
			}

			return providertest.Out{
				Provider: provider,
			}
		},
		func() {
			if db != nil {
				db.Close()
			}
		},
	)

	Describe("func Open()", func() {
		It("returns an error if the driver can not be deduced", func() {
			provider.DB = sqltest.MockDB()

			ds, err := provider.Open(
				context.Background(),
				configkit.FromApplication(&Application{
					ConfigureFunc: func(c dogma.ApplicationConfigurer) {
						c.Identity("<app-name>", "<app-key>")
					},
				}),
				Marshaler,
			)
			if ds != nil {
				ds.Close()
			}
			Expect(err).To(MatchError("can not deduce the appropriate SQL driver for *sqltest.MockDriver"))
		})
	})
})

var _ = Describe("type DSNProvider", func() {
	var (
		db       *sql.DB
		provider *DSNProvider
	)

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

			provider = &DSNProvider{
				DriverName: "sqlite3",
				DSN:        dsn,
			}

			return providertest.Out{
				Provider: provider,
			}
		},
		func() {
			if db != nil {
				db.Close()
			}
		},
	)

	Describe("func Open()", func() {
		It("returns an error if the DB can not be opened", func() {
			provider.DriverName = "<nonsense-driver>"
			provider.DSN = "<nonsense-dsn>"

			ds, err := provider.Open(
				context.Background(),
				configkit.FromApplication(&Application{
					ConfigureFunc: func(c dogma.ApplicationConfigurer) {
						c.Identity("<app-name>", "<app-key>")
					},
				}),
				Marshaler,
			)
			if ds != nil {
				ds.Close()
			}
			Expect(err).Should(HaveOccurred())
		})

		It("returns an error if the driver can not be deduced", func() {
			provider.DriverName = sqltest.MockDriverName()

			ds, err := provider.Open(
				context.Background(),
				configkit.FromApplication(&Application{
					ConfigureFunc: func(c dogma.ApplicationConfigurer) {
						c.Identity("<app-name>", "<app-key>")
					},
				}),
				Marshaler,
			)
			if ds != nil {
				ds.Close()
			}
			Expect(err).To(MatchError("can not deduce the appropriate SQL driver for *sqltest.MockDriver"))
		})
	})
})
