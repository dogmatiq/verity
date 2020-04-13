package offsetstore

import (
	"context"

	"github.com/dogmatiq/infix/persistence"
	"github.com/dogmatiq/infix/persistence/internal/providertest/common"
	"github.com/dogmatiq/infix/persistence/subsystem/offsetstore"
	"github.com/onsi/ginkgo"
	"github.com/onsi/gomega"
)

// DeclareRepositoryTests declares a functional test-suite for a specific
// offsetstore.Repository implementation.
func DeclareRepositoryTests(tc *common.TestContext) {
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
			ginkgo.It("loads the initial offset as zero", func() {
				o, err := repository.LoadOffset(context.Background(), "<source-app-key>")
				gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
				gomega.Expect(o).Should(gomega.BeNumerically("==", 0))
			})

			// TO-DO: add a test with transaction involved to test the
			// incremented offset.

			ginkgo.It("returns an error if the context is canceled", func() {
				ctx, cancel := context.WithCancel(tc.Context)
				cancel()

				_, err := repository.LoadOffset(ctx, "<app-key>")
				gomega.Expect(err).To(gomega.Equal(context.Canceled))
			})
		})
	})
}
