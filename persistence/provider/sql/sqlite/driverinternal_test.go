// +build cgo

package sqlite

import (
	"context"
	"time"

	"github.com/dogmatiq/sqltest"
	"github.com/dogmatiq/verity/internal/x/sqlx"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("type driver", func() {
	It("closes the database if application lock information is corrupted", func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
		defer cancel()

		database, err := sqltest.NewDatabase(ctx, sqltest.SQLite3Driver, sqltest.SQLite)
		Expect(err).ShouldNot(HaveOccurred())
		defer database.Close()

		db, err := database.Open()
		Expect(err).ShouldNot(HaveOccurred())

		d := driver{
			lockUpdateInterval: 10 * time.Millisecond,
		}

		err = d.DropSchema(ctx, db)
		Expect(err).ShouldNot(HaveOccurred())

		err = d.CreateSchema(ctx, db)
		Expect(err).ShouldNot(HaveOccurred())

		release, err := d.LockApplication(
			ctx,
			db,
			"<app-key>",
		)
		Expect(err).ShouldNot(HaveOccurred())
		defer release()

		sqlx.Exec(
			ctx,
			db,
			"DELETE FROM app_lock",
		)

		for {
			if err := db.PingContext(ctx); err != nil {
				// This operation should fail at some point, but not due to the
				// context timing out.
				Expect(err).To(MatchError("sql: database is closed"))
				return
			}

			time.Sleep(d.lockUpdateInterval)
		}
	})
})
