package persistence_test

import (
	"context"
	"errors"

	"github.com/dogmatiq/configkit"
	"github.com/dogmatiq/dogma"
	. "github.com/dogmatiq/dogma/fixtures"
	. "github.com/dogmatiq/infix/persistence"
	"github.com/dogmatiq/infix/persistence/provider/memory"
	"github.com/dogmatiq/marshalkit"
	. "github.com/dogmatiq/marshalkit/fixtures"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("type DataStoreSet", func() {
	var (
		provider *memory.Provider
		set      *DataStoreSet
		cfg      configkit.RichApplication
	)

	BeforeEach(func() {
		provider = &memory.Provider{}

		set = &DataStoreSet{
			Provider:  provider,
			Marshaler: Marshaler,
		}

		cfg = configkit.FromApplication(&Application{
			ConfigureFunc: func(c dogma.ApplicationConfigurer) {
				c.Identity("<app-name>", "<app-key>")
			},
		})
	})

	AfterEach(func() {
		set.Close()
	})

	Describe("func Get()", func() {
		It("opens a data-store", func() {
			ds, err := set.Get(context.Background(), cfg)
			Expect(err).ShouldNot(HaveOccurred())

			expect, err := provider.Open(context.Background(), cfg, set.Marshaler)
			Expect(err).ShouldNot(HaveOccurred())

			Expect(ds).To(Equal(expect))
		})

		It("returns the same instance on subsequent calls", func() {
			ds1, err := set.Get(context.Background(), cfg)
			Expect(err).ShouldNot(HaveOccurred())

			ds2, err := set.Get(context.Background(), cfg)
			Expect(err).ShouldNot(HaveOccurred())

			Expect(ds1).To(BeIdenticalTo(ds2))
		})

		It("returns an error if the provider cannot open the store", func() {
			set.Provider = failProvider{}

			_, err := set.Get(context.Background(), cfg)
			Expect(err).To(MatchError("<error>"))
		})
	})
})

type failProvider struct{}

func (failProvider) Open(
	context.Context,
	configkit.RichApplication,
	marshalkit.Marshaler,
) (DataStore, error) {
	return nil, errors.New("<error>")
}
