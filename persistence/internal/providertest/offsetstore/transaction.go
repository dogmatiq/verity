package offsetstore

import (
	"github.com/dogmatiq/infix/persistence"
	"github.com/dogmatiq/infix/persistence/internal/providertest/common"
	"github.com/dogmatiq/infix/persistence/subsystem/offsetstore"
	"github.com/onsi/ginkgo"
	"github.com/onsi/gomega"
)

// DeclareTransactionTests declares a functional test-suite for a specific
// offsetstore.Transaction implementation.
func DeclareTransactionTests(tc *common.TestContext) {
	ginkgo.Describe("type queuestore.Transaction", func() {
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

		ginkgo.Describe("func SaveOffset()", func() {
			ginkgo.It("saves an offset", func() {
				o := offsetstore.Offset(0)
				err := common.WithTransactionRollback(
					tc.Context,
					dataStore,
					func(tx persistence.ManagedTransaction) error {
						o := offsetstore.Offset(0)
						return tx.SaveOffset(tc.Context, "<app-key>", o)
					},
				)
				gomega.Expect(err).ShouldNot(gomega.HaveOccurred())

				actual, err := repository.LoadOffset(tc.Context, "<app-key>")
				gomega.Expect(err).ShouldNot(gomega.HaveOccurred())

				gomega.Expect(actual).Should(gomega.BeNumerically("==", o))
			})

			ginkgo.It("returns ErrConflict if the given offset does not equal the currently persisted offset", func() {
				err := common.WithTransactionRollback(
					tc.Context,
					dataStore,
					func(tx persistence.ManagedTransaction) error {
						return tx.SaveOffset(tc.Context, "<app-key>", offsetstore.Offset(999))
					},
				)
				gomega.Expect(err).Should(gomega.HaveOccurred())
				gomega.Expect(err).Should(gomega.MatchError(offsetstore.ErrConflict))
			})
		})
	})
}
