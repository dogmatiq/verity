package boltdb_test

import (
	"context"
	"time"

	"github.com/dogmatiq/configkit"
	"github.com/dogmatiq/dogma"
	. "github.com/dogmatiq/dogma/fixtures" // can't dot-import due to conflicts
	"github.com/dogmatiq/infix/internal/testing/boltdbtest"
	"github.com/dogmatiq/infix/persistence/internal/providertest"
	. "github.com/dogmatiq/infix/persistence/provider/boltdb"
	. "github.com/dogmatiq/marshalkit/fixtures"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"go.etcd.io/bbolt"
)

var _ = Describe("type Provider", func() {
	var (
		db    *bbolt.DB
		close func()
	)

	providertest.Declare(
		func(ctx context.Context, in providertest.In) providertest.Out {
			db, close = boltdbtest.Open()

			return providertest.Out{
				Provider: &Provider{db},
			}
		},
		func() {
			if close != nil {
				close()
			}
		},
	)
})

var _ = Describe("type FileProvider", func() {
	var (
		db       *bbolt.DB
		close    func()
		provider *FileProvider
	)

	providertest.Declare(
		func(ctx context.Context, in providertest.In) providertest.Out {
			db, close = boltdbtest.Open()

			provider = &FileProvider{
				Path: db.Path(), // capture the temp path of the DB.
			}

			db.Close() // close the original DB so that the file is not locked.

			return providertest.Out{
				Provider: provider,
			}
		},
		func() {
			if close != nil {
				close()
			}
		},
	)

	Describe("func Open()", func() {
		It("returns an error if the DB can not be opened", func() {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
			defer cancel()

			// Open the database file first so that the file is locked.
			db, err := bbolt.Open(provider.Path, 0600, nil)
			Expect(err).ShouldNot(HaveOccurred())
			defer db.Close()

			_, err = provider.Open(
				ctx,
				configkit.FromApplication(&Application{
					ConfigureFunc: func(c dogma.ApplicationConfigurer) {
						c.Identity("<app-name>", "<app-key>")
					},
				}),
				Marshaler,
			)
			Expect(err).To(Equal(context.DeadlineExceeded))
		})
	})
})
