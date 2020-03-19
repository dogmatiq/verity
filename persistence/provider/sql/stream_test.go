// +build cgo

package sql_test

import (
	"context"
	"database/sql"
	"time"

	. "github.com/dogmatiq/configkit/fixtures"
	"github.com/dogmatiq/configkit/message"
	. "github.com/dogmatiq/dogma/fixtures"
	"github.com/dogmatiq/infix/envelope"
	"github.com/dogmatiq/infix/fixtures" // can't dot-import due to conflicts
	"github.com/dogmatiq/infix/internal/testing/sqltest"
	"github.com/dogmatiq/infix/internal/testing/streamtest"
	"github.com/dogmatiq/infix/internal/x/sqlx"
	. "github.com/dogmatiq/infix/persistence/provider/sql"
	"github.com/dogmatiq/infix/persistence/provider/sql/driver/sqlite"
	"github.com/dogmatiq/linger/backoff"
	. "github.com/dogmatiq/marshalkit/fixtures"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("type Stream (standard test suite)", func() {
	var db *sql.DB

	streamtest.Declare(
		func(ctx context.Context, in streamtest.In) streamtest.Out {
			db = sqltest.Open("sqlite3")

			err := sqlite.DropSchema(ctx, db)
			Expect(err).ShouldNot(HaveOccurred())

			err = sqlite.CreateSchema(ctx, db)
			Expect(err).ShouldNot(HaveOccurred())

			stream := &Stream{
				AppKey:          in.ApplicationKey,
				DB:              db,
				Driver:          sqlite.StreamDriver{},
				Types:           in.MessageTypes,
				Marshaler:       in.Marshaler,
				BackoffStrategy: backoff.Constant(10 * time.Millisecond),
			}

			return streamtest.Out{
				Stream: stream,
				Append: func(ctx context.Context, envelopes ...*envelope.Envelope) {
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
			}
		},
		func() {
			if db != nil {
				db.Close()
			}
		},
	)
})

var _ = Describe("type Stream", func() {
	var (
		ctx    context.Context
		cancel context.CancelFunc
		db     *sql.DB
	)

	BeforeEach(func() {
		ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)

		db = sqltest.Open("sqlite3")

		err := sqlite.DropSchema(ctx, db)
		Expect(err).ShouldNot(HaveOccurred())

		err = sqlite.CreateSchema(ctx, db)
		Expect(err).ShouldNot(HaveOccurred())
	})

	AfterEach(func() {
		if db != nil {
			db.Close()
		}

		cancel()
	})

	Describe("func Append()", func() {
		It("panics if the message type is not supported", func() {
			env := fixtures.NewEnvelope("<id>", MessageA1)

			stream := &Stream{
				AppKey: "<app-key>",
				DB:     db,
				Driver: sqlite.StreamDriver{},
				Types: message.NewTypeSet(
					MessageBType,
				),
				Marshaler: Marshaler,
			}

			tx := sqlx.Begin(ctx, db)
			defer tx.Rollback()

			Expect(func() {
				stream.Append(ctx, tx, env)
			}).To(Panic())
		})
	})
})
