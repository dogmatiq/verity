package providertest

import (
	"context"

	"github.com/dogmatiq/infix/persistence"
	"github.com/dogmatiq/infix/persistence/subsystem/offsetstore"
	"github.com/onsi/ginkgo"
	"github.com/onsi/gomega"
)

// declareOffsetRepositoryTests declares a functional test-suite for a specific
// offsetstore.Repository implementation.
func declareOffsetRepositoryTests(tc *TestContext) {
	ginkgo.Describe("type offsetstore.Repository", func() {
		var (
			dataStore  persistence.DataStore
			repository offsetstore.Repository
			tearDown   func()
		)

		ginkgo.BeforeEach(func() {
			dataStore, tearDown = tc.SetupDataStore()
			repository = dataStore.OffsetStoreRepository()
		})

		ginkgo.AfterEach(func() {
			tearDown()
		})

		ginkgo.Describe("func LoadOffset()", func() {
			ginkgo.When("application has no previous offsets associated", func() {
				ginkgo.It("loads the initial offset as zero", func() {
					actual, err := repository.LoadOffset(
						tc.Context,
						"<source-app-key>",
					)
					gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
					gomega.Expect(actual).To(gomega.BeEquivalentTo(0))
				})
			})

			ginkgo.When("application has previous offsets associated", func() {
				ginkgo.It("returns the current offset", func() {
					persist(
						tc.Context,
						dataStore,
						persistence.SaveOffset{
							ApplicationKey: "<source-app-key>",
							CurrentOffset:  0,
							NextOffset:     1,
						},
					)

					actual, err := repository.LoadOffset(
						tc.Context,
						"<source-app-key>",
					)
					gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
					gomega.Expect(actual).To(gomega.BeEquivalentTo(1))
				})
			})

			ginkgo.When("context is cancelled", func() {
				ginkgo.It("returns the context cancellation error", func() {
					ctx, cancel := context.WithCancel(tc.Context)
					cancel()

					_, err := repository.LoadOffset(ctx, "<source-app-key>")
					gomega.Expect(err).To(gomega.Equal(context.Canceled))
				})
			})
		})
	})
}
