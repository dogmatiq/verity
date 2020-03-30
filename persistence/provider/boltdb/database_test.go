package boltdb_test

import (
	"context"
	"time"

	"github.com/dogmatiq/infix/internal/testing/boltdbtest"
	. "github.com/dogmatiq/infix/persistence/provider/boltdb"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("type database", func() {
	Describe("func Begin()", func() {
		It("fails if the underlying database is closed", func() {
			ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			defer cancel()

			db, close := boltdbtest.Open()
			defer close()

			provider := &Provider{
				DB: db,
			}

			ds, err := provider.Open(ctx, "<app-key>")
			Expect(err).ShouldNot(HaveOccurred())
			defer ds.Close()

			close() // Close the underlying BoltDB database.

			tx, err := ds.Begin(ctx)
			Expect(err).ShouldNot(HaveOccurred())
			defer tx.Rollback()

			// Perform a write to cause the actual transaction to be started.
			_, err = tx.SaveEvent(ctx, nil)
			Expect(err).Should(HaveOccurred())
		})
	})
})
