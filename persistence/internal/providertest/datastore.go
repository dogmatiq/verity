package providertest

import (
	"github.com/dogmatiq/infix/persistence"
	"github.com/onsi/ginkgo"
	"github.com/onsi/gomega"
)

func declareDataStoreTests(tc *TestContext) {
	ginkgo.Describe("type DataStore (interface)", func() {
		var (
			provider      persistence.Provider
			closeProvider func()
			dataStore     persistence.DataStore
		)

		ginkgo.BeforeEach(func() {
			provider, closeProvider = tc.Out.NewProvider()

			var err error
			dataStore, err = provider.Open(tc.Context, "<app-key>")
			gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
		})

		ginkgo.AfterEach(func() {
			if dataStore != nil {
				dataStore.Close()
			}

			if closeProvider != nil {
				closeProvider()
			}
		})

		ginkgo.Describe("func QueueStoreRepository()", func() {
			ginkgo.It("returns a non-nil repository", func() {
				r := dataStore.QueueStoreRepository()
				gomega.Expect(r).NotTo(gomega.BeNil())
			})
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
							ApplicationKey: "<app-key>",
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
