//go:build cgo
// +build cgo

package sqlpersistence_test

import (
	"context"
	"database/sql"
	"time"

	"github.com/dogmatiq/sqltest"
	"github.com/dogmatiq/sqltest/sqlstub"
	. "github.com/dogmatiq/verity/fixtures"
	"github.com/dogmatiq/verity/persistence"
	"github.com/dogmatiq/verity/persistence/internal/providertest"
	. "github.com/dogmatiq/verity/persistence/sqlpersistence"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"go.uber.org/multierr"
)

var _ = Describe("type Provider", func() {
	var (
		database *sqltest.Database
		db       *sql.DB
	)

	providertest.Declare(
		func(ctx context.Context, in providertest.In) providertest.Out {
			var err error
			database, err = sqltest.NewDatabase(ctx, sqltest.SQLite3Driver, sqltest.SQLite)
			Expect(err).ShouldNot(HaveOccurred())

			db, err = database.Open()
			Expect(err).ShouldNot(HaveOccurred())

			err = CreateSchema(ctx, db)
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
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()

			err := DropSchema(ctx, db)
			Expect(err).ShouldNot(HaveOccurred())

			err = database.Close()
			Expect(err).ShouldNot(HaveOccurred())
		},
	)
})

var _ = Describe("type DSNProvider", func() {
	var database *sqltest.Database

	providertest.Declare(
		func(ctx context.Context, in providertest.In) providertest.Out {
			var err error
			database, err = sqltest.NewDatabase(ctx, sqltest.SQLite3Driver, sqltest.SQLite)
			Expect(err).ShouldNot(HaveOccurred())

			db, err := database.Open()
			Expect(err).ShouldNot(HaveOccurred())
			defer db.Close()

			err = CreateSchema(ctx, db)
			Expect(err).ShouldNot(HaveOccurred())

			return providertest.Out{
				NewProvider: func() (persistence.Provider, func()) {
					return &DSNProvider{
						DriverName: database.DataSource.DriverName(),
						DSN:        database.DataSource.DSN(),
					}, nil
				},
				IsShared: true,
			}
		},
		func() {
			err := database.Close()
			Expect(err).ShouldNot(HaveOccurred())
		},
	)

	Describe("func Open()", func() {
		It("returns an error if the DB can not be opened", func() {
			provider := &DSNProvider{
				DriverName: "<nonsense-driver>",
				DSN:        "<nonsense-dsn>",
			}

			ds, err := provider.Open(context.Background(), DefaultAppKey)
			if ds != nil {
				ds.Close()
			}
			Expect(err).Should(HaveOccurred())
		})
	})

	Context("const DefaultMaxIdleConns", func() {
		It("is not zero", func() {
			Expect(DefaultMaxIdleConns).To(BeNumerically(">", 0))
		})
	})

	Context("const DefaultMaxOpenConns", func() {
		It("is larger than DefaultMaxIdleConns", func() {
			Expect(DefaultMaxOpenConns).To(BeNumerically(">", DefaultMaxIdleConns))
		})
	})

	Context("const DefaultMaxConnLifetime", func() {
		It("is not zero", func() {
			Expect(DefaultMaxConnLifetime).To(BeNumerically(">", 0))
		})
	})
})

var _ = Describe("type provider", func() {
	Describe("func open()", func() {
		It("returns an error if a compatible driver can not be found", func() {
			provider := &Provider{
				DB: sql.OpenDB(&sqlstub.Connector{}),
			}

			ds, err := provider.Open(context.Background(), DefaultAppKey)
			if ds != nil {
				ds.Close()
			}

			expect := "could not find a driver that is compatible with *sqlstub.Driver"
			for _, e := range multierr.Errors(err) {
				if e.Error() == expect {
					return
				}
			}

			Expect(err).To(MatchError(expect))
		})
	})
})
