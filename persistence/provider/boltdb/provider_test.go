package boltdb_test

import (
	"context"
	"time"

	"github.com/dogmatiq/infix/internal/testing/boltdbtest"
	"github.com/dogmatiq/infix/persistence"
	. "github.com/dogmatiq/infix/persistence/provider/boltdb"
	"github.com/dogmatiq/infix/persistence/provider/internal/providertest"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("type Provider", func() {
	providertest.Declare(
		func(ctx context.Context, in providertest.In) providertest.Out {
			return providertest.Out{
				NewProvider: func() (persistence.Provider, func()) {
					db, close := boltdbtest.Open()

					return &Provider{
						DB: db,
					}, close
				},
			}
		},
		nil,
	)
})

var _ = Describe("type FileProvider", func() {
	providertest.Declare(
		func(ctx context.Context, in providertest.In) providertest.Out {
			return providertest.Out{
				NewProvider: func() (persistence.Provider, func()) {
					db, close := boltdbtest.Open()

					path := db.Path() // capture the temp path of the DB.
					db.Close()        // close the original DB so that the file is not locked.

					return &FileProvider{
						Path: path,
					}, close
				},
			}
		},
		nil,
	)

	Describe("func Open()", func() {
		It("returns an error if the DB can not be opened", func() {
			db, close := boltdbtest.Open()
			defer close()

			provider := &FileProvider{
				Path: db.Path(), // use the same file as the (open) DB.
			}

			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
			defer cancel()

			ds, err := provider.Open(ctx, "<app-key>")
			if ds != nil {
				ds.Close()
			}
			Expect(err).To(Equal(context.DeadlineExceeded))
		})
	})
})
