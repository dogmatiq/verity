package mysql_test

import (
	"context"
	"database/sql"

	"github.com/dogmatiq/verity/internal/testing/sqltest"
	"github.com/dogmatiq/verity/persistence"
	"github.com/dogmatiq/verity/persistence/internal/providertest"
	veritysql "github.com/dogmatiq/verity/persistence/provider/sql"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("type driver", func() {
	var (
		db      *sql.DB
		closeDB func()
	)

	providertest.Declare(
		func(ctx context.Context, in providertest.In) providertest.Out {
			db, _, closeDB = sqltest.Open("mysql")

			d, err := veritysql.NewDriver(db)
			Expect(err).ShouldNot(HaveOccurred())

			err = d.DropSchema(ctx, db)
			Expect(err).ShouldNot(HaveOccurred())

			err = d.CreateSchema(ctx, db)
			Expect(err).ShouldNot(HaveOccurred())

			return providertest.Out{
				NewProvider: func() (persistence.Provider, func()) {
					return &veritysql.Provider{
						DB:     db,
						Driver: d,
					}, nil
				},
				IsShared: true,
			}
		},
		func() {
			if closeDB != nil {
				closeDB()
			}
		},
	)
})
