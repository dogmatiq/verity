// +build cgo

package sqlite_test

import (
	"context"
	"database/sql"

	"github.com/dogmatiq/infix/internal/testing/sqltest"
	"github.com/dogmatiq/infix/persistence/internal/providertest"
	infixsql "github.com/dogmatiq/infix/persistence/provider/sql"
	. "github.com/dogmatiq/infix/persistence/provider/sql/driver/sqlite"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Context("SQLite driver", func() {
	var db *sql.DB

	providertest.Declare(
		func(ctx context.Context, in providertest.In) providertest.Out {
			db = sqltest.Open("sqlite3")

			err := DropSchema(ctx, db)
			Expect(err).ShouldNot(HaveOccurred())

			err = CreateSchema(ctx, db)
			Expect(err).ShouldNot(HaveOccurred())

			return providertest.Out{
				Provider: &infixsql.Provider{
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