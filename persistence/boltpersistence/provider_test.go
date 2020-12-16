package boltpersistence_test

import (
	"context"
	"io/ioutil"
	"os"
	"sync"
	"time"

	"github.com/dogmatiq/verity/persistence"
	. "github.com/dogmatiq/verity/persistence/boltpersistence"
	"github.com/dogmatiq/verity/persistence/internal/providertest"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"go.etcd.io/bbolt"
)

var _ = Describe("type Provider", func() {
	providertest.Declare(
		func(ctx context.Context, in providertest.In) providertest.Out {
			return providertest.Out{
				NewProvider: func() (persistence.Provider, func()) {
					db, close := openTemp()

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
					db, close := openTemp()

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
			db, close := openTemp()
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

// openTemp opens a BoltDB database using a temporary file.
//
// The returned function must be used to close the database, instead of
// DB.Close().
func openTemp() (*bbolt.DB, func()) {
	filename, remove := tempFile()

	db, err := bbolt.Open(filename, 0600, nil)
	if err != nil {
		panic(err)
	}

	return db, func() {
		db.Close()
		remove()
	}
}

// tempFile returns the name of a temporary file to be used for a BoltDB
// database.
//
// It returns a function that deletes the temporary file.
func tempFile() (string, func()) {
	f, err := ioutil.TempFile("", "*.boltdb")
	if err != nil {
		panic(err)
	}

	if err := f.Close(); err != nil {
		panic(err)
	}

	file := f.Name()

	if err := os.Remove(file); err != nil {
		panic(err)
	}

	var once sync.Once
	return file, func() {
		once.Do(func() {
			os.Remove(file)
		})
	}
}
