package mysql_test

import (
	"context"
	"database/sql"

	"github.com/dogmatiq/infix/internal/testing/sqltest"
	"github.com/dogmatiq/infix/persistence"
	"github.com/dogmatiq/infix/persistence/provider/internal/providertest"
	infixsql "github.com/dogmatiq/infix/persistence/provider/sql"
	. "github.com/dogmatiq/infix/persistence/provider/sql/mysql"
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

			err := DropSchema(ctx, db)
			Expect(err).ShouldNot(HaveOccurred())

			err = CreateSchema(ctx, db)
			Expect(err).ShouldNot(HaveOccurred())

			return providertest.Out{
				NewProvider: func() (persistence.Provider, func()) {
					return &infixsql.Provider{
						DB: db,
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
