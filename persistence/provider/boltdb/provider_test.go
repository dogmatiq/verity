package boltdb_test

import (
	"context"
	"io/ioutil"
	"os"
	"time"

	"github.com/dogmatiq/configkit"
	"github.com/dogmatiq/infix/persistence"
	. "github.com/dogmatiq/infix/persistence/provider/boltdb"
	. "github.com/dogmatiq/marshalkit/fixtures"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("type Provider", func() {
	var (
		ctx      context.Context
		cancel   context.CancelFunc
		tmpfile  string
		provider *Provider
	)

	BeforeEach(func() {
		ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
		defer cancel()

		f, err := ioutil.TempFile("", "*.boltdb")
		Expect(err).ShouldNot(HaveOccurred())
		f.Close()
		tmpfile = f.Name()

		provider = &Provider{
			File: tmpfile,
		}
	})

	AfterEach(func() {
		cancel()

		if tmpfile != "" {
			os.Remove(tmpfile)
		}
	})

	Describe("type dataStore", func() {
		var store persistence.DataStore

		BeforeEach(func() {
			var err error
			store, err = provider.Open(
				ctx,
				configkit.MustNewIdentity("<app>", "<app-key>"),
				Marshaler,
			)
			Expect(err).ShouldNot(HaveOccurred())
		})

		Describe("func Stream()", func() {
			It("returns a stream for the given application", func() {
				stream, err := store.EventStream(ctx)
				Expect(err).ShouldNot(HaveOccurred())
				Expect(stream).To(BeAssignableToTypeOf(
					(*Stream)(nil),
				))
			})
		})
	})
})
