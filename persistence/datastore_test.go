package persistence_test

import (
	"context"
	"errors"

	. "github.com/dogmatiq/verity/fixtures"
	. "github.com/dogmatiq/verity/persistence"
	"github.com/dogmatiq/verity/persistence/memorypersistence"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("type DataStoreSet", func() {
	var (
		ctx      = context.Background()
		provider *ProviderStub
		set      *DataStoreSet
	)

	BeforeEach(func() {
		provider = &ProviderStub{
			Provider: &memorypersistence.Provider{},
		}

		set = &DataStoreSet{
			Provider: provider,
		}
	})

	AfterEach(func() {
		set.Close()
	})

	Describe("func Get()", func() {
		It("opens a data-store", func() {
			expect, err := provider.Open(ctx, DefaultAppKey)
			Expect(err).ShouldNot(HaveOccurred())

			provider.OpenFunc = func(
				_ context.Context,
				k string,
			) (DataStore, error) {
				Expect(k).To(Equal(DefaultAppKey))
				return expect, nil
			}

			ds, err := set.Get(ctx, DefaultAppKey)
			Expect(err).ShouldNot(HaveOccurred())
			Expect(ds).To(Equal(expect))
		})

		It("returns the same instance on subsequent calls", func() {
			ds1, err := set.Get(ctx, DefaultAppKey)
			Expect(err).ShouldNot(HaveOccurred())

			ds2, err := set.Get(ctx, DefaultAppKey)
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

			ds, err := set.Get(ctx, DefaultAppKey)
			if ds != nil {
				ds.Close()
			}
			Expect(err).To(MatchError("<error>"))
		})
	})
})
