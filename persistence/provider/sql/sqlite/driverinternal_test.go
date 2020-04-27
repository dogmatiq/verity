// +build cgo

package sqlite

import (
	"context"
	"time"

	"github.com/dogmatiq/infix/internal/testing/sqltest"
	"github.com/dogmatiq/infix/internal/x/sqlx"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("type driver", func() {
	It("closes the database if application lock information is corrupted", func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
		defer cancel()

		db, _, close := sqltest.Open("sqlite3")
		defer close()

		err := DropSchema(ctx, db)
		Expect(err).ShouldNot(HaveOccurred())

		err = CreateSchema(ctx, db)
		Expect(err).ShouldNot(HaveOccurred())

		d := driver{
			lockUpdateInterval: 10 * time.Millisecond,
		}

		release, err := d.LockApplication(
			ctx,
			db,
			"<app-key>",
		)
		Expect(err).ShouldNot(HaveOccurred())
		defer release() // nolint

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
