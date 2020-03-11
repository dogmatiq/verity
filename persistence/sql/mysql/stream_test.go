package mysql_test

import (
	"context"
	"database/sql"
	"time"

	"github.com/dogmatiq/infix/envelope"
	"github.com/dogmatiq/infix/internal/x/sqlx"
	"github.com/dogmatiq/infix/persistence"
	"github.com/dogmatiq/infix/persistence/internal/streamtest"
	infixsql "github.com/dogmatiq/infix/persistence/sql"
	"github.com/dogmatiq/infix/persistence/sql/internal/drivertest"
	. "github.com/dogmatiq/infix/persistence/sql/mysql"
	"github.com/dogmatiq/linger/backoff"
	"github.com/dogmatiq/marshalkit"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("type StreamDriver (via standard stream test suite)", func() {
	var (
		stream *infixsql.Stream
		db     *sql.DB
	)

	streamtest.Declare(
		func(ctx context.Context, m marshalkit.Marshaler) persistence.Stream {
			db = drivertest.Open("mysql")

			err := DropSchema(ctx, db)
			Expect(err).ShouldNot(HaveOccurred())

			err = CreateSchema(ctx, db)
			Expect(err).ShouldNot(HaveOccurred())

			stream = &infixsql.Stream{
				ApplicationKey:  "<app-key>",
				DB:              db,
				Marshaler:       m,
				Driver:          StreamDriver{},
				BackoffStrategy: backoff.Constant(10 * time.Millisecond),
			}

			return stream
		},
		func() {
			if db != nil {
				db.Close()
			}
		},
		func(ctx context.Context, envelopes ...*envelope.Envelope) {
			tx := sqlx.Begin(ctx, db)
			defer tx.Rollback()

			_, err := stream.Append(
				ctx,
				tx,
				envelopes...,
			)
			Expect(err).ShouldNot(HaveOccurred())

			sqlx.Commit(tx)
		},
	)
})
