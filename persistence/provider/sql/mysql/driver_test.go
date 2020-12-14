package mysql_test

import (
	"context"
	"database/sql"
	"time"

	"github.com/dogmatiq/sqltest"
	"github.com/dogmatiq/verity/persistence"
	"github.com/dogmatiq/verity/persistence/internal/providertest"
	veritysql "github.com/dogmatiq/verity/persistence/provider/sql"
	. "github.com/dogmatiq/verity/persistence/provider/sql/mysql"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("type driver", func() {
	var (
		database *sqltest.Database
		db       *sql.DB
	)

	providertest.Declare(
		func(ctx context.Context, in providertest.In) providertest.Out {
			var err error
			database, err = sqltest.NewDatabase(ctx, sqltest.MySQLDriver, sqltest.MySQL)
			Expect(err).ShouldNot(HaveOccurred())

			db, err = database.Open()
			Expect(err).ShouldNot(HaveOccurred())

			err = Driver.CreateSchema(ctx, db)
			Expect(err).ShouldNot(HaveOccurred())

			return providertest.Out{
				NewProvider: func() (persistence.Provider, func()) {
					return &veritysql.Provider{
						DB: db,
					}, nil
				},
				IsShared: true,
			}
		},
		func() {
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()

			err := Driver.DropSchema(ctx, db)
			Expect(err).ShouldNot(HaveOccurred())

			err = database.Close()
			Expect(err).ShouldNot(HaveOccurred())
		},
	)
})
