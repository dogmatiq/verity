package providertest

import (
	"sync"

	"github.com/dogmatiq/verity/fixtures"
	"github.com/dogmatiq/verity/persistence"
	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
)

// declareOffsetOperationTests declares a functional test-suite for
// persistence operations related to storing offsets.
func declareOffsetOperationTests(tc *TestContext) {
	ginkgo.Context("offset operations", func() {
		var dataStore persistence.DataStore

		ginkgo.BeforeEach(func() {
			var tearDown func()
			dataStore, tearDown = tc.SetupDataStore()
			ginkgo.DeferCleanup(tearDown)
		})

		ginkgo.Describe("type persistence.SaveOffset", func() {
			ginkgo.When("application has no previous offsets associated", func() {
				ginkgo.It("saves an offset", func() {
					persist(
						tc.Context,
						dataStore,
						persistence.SaveOffset{
							ApplicationKey: fixtures.DefaultAppKey,
							CurrentOffset:  0,
							NextOffset:     1,
						},
					)

					actual := loadOffset(tc.Context, dataStore, fixtures.DefaultAppKey)
					gomega.Expect(actual).To(gomega.BeEquivalentTo(1))
				})

				ginkgo.It("it does not update the offset when an OCC conflict occurs", func() {
					op := persistence.SaveOffset{
						ApplicationKey: fixtures.DefaultAppKey,
						CurrentOffset:  1,
						NextOffset:     2,
					}

					_, err := dataStore.Persist(
						tc.Context,
						persistence.Batch{op},
					)
					gomega.Expect(err).To(gomega.Equal(
						persistence.ConflictError{
							Cause: op,
						},
					))

					actual := loadOffset(tc.Context, dataStore, fixtures.DefaultAppKey)
					gomega.Expect(actual).To(gomega.BeEquivalentTo(0))
				})
			})

			ginkgo.When("application has offsets associated", func() {
				ginkgo.BeforeEach(func() {
					persist(
						tc.Context,
						dataStore,
						persistence.SaveOffset{
							ApplicationKey: fixtures.DefaultAppKey,
							CurrentOffset:  0,
							NextOffset:     5,
						},
					)
				})

				ginkgo.It("updates the offset", func() {
					persist(
						tc.Context,
						dataStore,
						persistence.SaveOffset{
							ApplicationKey: fixtures.DefaultAppKey,
							CurrentOffset:  5,
							NextOffset:     123,
						},
					)

					actual := loadOffset(tc.Context, dataStore, fixtures.DefaultAppKey)
					gomega.Expect(actual).To(gomega.BeEquivalentTo(123))
				})

				ginkgo.DescribeTable(
					"it does not update the offset when an OCC conflict occurs",
					func(conflictingOffset int) {
						op := persistence.SaveOffset{
							ApplicationKey: fixtures.DefaultAppKey,
							CurrentOffset:  uint64(conflictingOffset),
							NextOffset:     123,
						}

						_, err := dataStore.Persist(
							tc.Context,
							persistence.Batch{op},
						)
						gomega.Expect(err).To(gomega.Equal(
							persistence.ConflictError{
								Cause: op,
							},
						))

						actual := loadOffset(tc.Context, dataStore, fixtures.DefaultAppKey)
						gomega.Expect(actual).To(gomega.BeEquivalentTo(5))
					},
					ginkgo.Entry("zero", 0),
					ginkgo.Entry("too low", 1),
					ginkgo.Entry("too high", 100),
				)
			})

			ginkgo.It("serializes operations from concurrent persist calls", func() {
				var g sync.WaitGroup

				fn := func(ak string, count uint64) {
					defer ginkgo.GinkgoRecover()
					defer g.Done()

					for i := uint64(0); i < count; i++ {
						persist(
							tc.Context,
							dataStore,
							persistence.SaveOffset{
								ApplicationKey: ak,
								CurrentOffset:  i,
								NextOffset:     i + 1,
							},
						)
					}
				}

				g.Add(3)
				go fn("<source-app1-key>", 1)
				go fn("<source-app2-key>", 2)
				go fn("<source-app3-key>", 3)
				g.Wait()

				actual := loadOffset(tc.Context, dataStore, "<source-app1-key>")
				gomega.Expect(actual).To(gomega.BeEquivalentTo(1))

				actual = loadOffset(tc.Context, dataStore, "<source-app2-key>")
				gomega.Expect(actual).To(gomega.BeEquivalentTo(2))

				actual = loadOffset(tc.Context, dataStore, "<source-app3-key>")
				gomega.Expect(actual).To(gomega.BeEquivalentTo(3))
			})
		})
	})
}
