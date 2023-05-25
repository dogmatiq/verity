package providertest

import (
	"github.com/dogmatiq/verity/fixtures"
	"github.com/dogmatiq/verity/persistence"
	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
)

func declareDataStoreTests(tc *TestContext) {
	ginkgo.Describe("type DataStore (interface)", func() {
		var (
			provider  persistence.Provider
			dataStore persistence.DataStore
		)

		ginkgo.BeforeEach(func() {
			var close func()
			provider, close = tc.Out.NewProvider()
			if close != nil {
				ginkgo.DeferCleanup(close)
			}

			var err error
			dataStore, err = provider.Open(tc.Context, fixtures.DefaultAppKey)
			gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
			ginkgo.DeferCleanup(func() { dataStore.Close() })
		})

		ginkgo.Describe("func Close()", func() {
			ginkgo.It("returns an error if the data-store is already closed", func() {
				err := dataStore.Close()
				gomega.Expect(err).ShouldNot(gomega.HaveOccurred())

				err = dataStore.Close()
				gomega.Expect(err).To(gomega.Equal(persistence.ErrDataStoreClosed))
			})

			ginkgo.It("prevents operations from being persisted", func() {
				err := dataStore.Close()
				gomega.Expect(err).ShouldNot(gomega.HaveOccurred())

				_, err = dataStore.Persist(
					tc.Context,
					persistence.Batch{
						persistence.SaveOffset{
							ApplicationKey: fixtures.DefaultAppKey,
							CurrentOffset:  0,
							NextOffset:     1,
						},
					},
				)
				gomega.Expect(err).To(gomega.Equal(persistence.ErrDataStoreClosed))
			})
		})
	})
}
