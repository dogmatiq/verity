package providertest

import (
	"context"

	"github.com/dogmatiq/verity/fixtures"
	"github.com/dogmatiq/verity/persistence"
	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
)

// declareOffsetRepositoryTests declares a functional test-suite for a specific
// persistence.OffsetRepository implementation.
func declareOffsetRepositoryTests(tc *TestContext) {
	ginkgo.Describe("type persistence.OffsetRepository", func() {
		var dataStore persistence.DataStore

		ginkgo.BeforeEach(func() {
			var tearDown func()
			dataStore, tearDown = tc.SetupDataStore()
			ginkgo.DeferCleanup(tearDown)
		})

		ginkgo.Describe("func LoadOffset()", func() {
			ginkgo.When("application has no previous offsets associated", func() {
				ginkgo.It("loads the initial offset as zero", func() {
					actual, err := dataStore.LoadOffset(
						tc.Context,
						fixtures.DefaultAppKey,
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
							ApplicationKey: fixtures.DefaultAppKey,
							CurrentOffset:  0,
							NextOffset:     1,
						},
					)

					actual, err := dataStore.LoadOffset(
						tc.Context,
						fixtures.DefaultAppKey,
					)
					gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
					gomega.Expect(actual).To(gomega.BeEquivalentTo(1))
				})
			})

			ginkgo.It("does not block if the context is canceled", func() {
				// This test ensures that the implementation returns
				// immediately, either with a context.Canceled error, or with
				// the correct result.

				persist(
					tc.Context,
					dataStore,
					persistence.SaveOffset{
						ApplicationKey: fixtures.DefaultAppKey,
						CurrentOffset:  0,
						NextOffset:     1,
					},
				)

				ctx, cancel := context.WithCancel(tc.Context)
				cancel()

				actual, err := dataStore.LoadOffset(ctx, fixtures.DefaultAppKey)
				if err != nil {
					gomega.Expect(err).To(gomega.Equal(context.Canceled))
				} else {
					gomega.Expect(actual).To(gomega.BeEquivalentTo(1))
				}
			})
		})
	})
}
