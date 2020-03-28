package persistence_test

import (
	"context"
	"errors"

	. "github.com/dogmatiq/infix/fixtures"
	. "github.com/dogmatiq/infix/persistence"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("type DataStoreSet", func() {
	var (
		ctx      = context.Background()
		provider *PersistenceProvider
		set      *DataStoreSet
	)

	BeforeEach(func() {
		provider = &PersistenceProvider{}

		set = &DataStoreSet{
			Provider: provider,
		}
	})

	AfterEach(func() {
		set.Close()
	})

	Describe("func Get()", func() {
		It("opens a data-store", func() {
			expect, err := provider.Memory.Open(ctx, "<app-key>")
			Expect(err).ShouldNot(HaveOccurred())

			provider.OpenFunc = func(
				_ context.Context,
				k string,
			) (DataStore, error) {
				Expect(k).To(Equal("<app-key>"))
				return expect, nil
			}

			ds, err := set.Get(ctx, "<app-key>")
			Expect(err).ShouldNot(HaveOccurred())
			Expect(ds).To(Equal(expect))
		})

		It("returns the same instance on subsequent calls", func() {
			ds1, err := set.Get(ctx, "<app-key>")
			Expect(err).ShouldNot(HaveOccurred())

			ds2, err := set.Get(ctx, "<app-key>")
			Expect(err).ShouldNot(HaveOccurred())

			Expect(ds1).To(BeIdenticalTo(ds2))
		})

		It("returns an error if the provider cannot open the store", func() {
			provider.OpenFunc = func(
				context.Context,
				string,
			) (DataStore, error) {
				return nil, errors.New("<error>")
			}

			ds, err := set.Get(ctx, "<app-key>")
			if ds != nil {
				ds.Close()
			}
			Expect(err).To(MatchError("<error>"))
		})
	})
})
