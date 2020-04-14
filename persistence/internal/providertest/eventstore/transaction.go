package eventstore

import (
	"context"
	"sort"
	"sync"
	"time"

	dogmafixtures "github.com/dogmatiq/dogma/fixtures"
	"github.com/dogmatiq/infix/draftspecs/envelopespec"
	infixfixtures "github.com/dogmatiq/infix/fixtures"
	"github.com/dogmatiq/infix/persistence"
	"github.com/dogmatiq/infix/persistence/internal/providertest/common"
	"github.com/dogmatiq/infix/persistence/subsystem/eventstore"
	"github.com/onsi/ginkgo"
	"github.com/onsi/gomega"
)

// DeclareTransactionTests declares a functional test-suite for a specific
// eventstore.Transaction implementation.
func DeclareTransactionTests(tc *common.TestContext) {
	ginkgo.Describe("type eventstore.Transaction", func() {
		var (
			dataStore  persistence.DataStore
			repository eventstore.Repository
			tearDown   func()

			env0, env1, env2 *envelopespec.Envelope
		)

		ginkgo.BeforeEach(func() {
			dataStore, tearDown = tc.SetupDataStore()
			repository = dataStore.EventStoreRepository()

			env0 = infixfixtures.NewEnvelopeProto("<message-0>", dogmafixtures.MessageA1)
			env1 = infixfixtures.NewEnvelopeProto("<message-1>", dogmafixtures.MessageB1)
			env2 = infixfixtures.NewEnvelopeProto("<message-2>", dogmafixtures.MessageC1)
		})

		ginkgo.AfterEach(func() {
			tearDown()
		})

		ginkgo.Describe("func SaveEvent()", func() {
			ginkgo.It("returns the offset of the event", func() {
				o := saveEvent(tc.Context, dataStore, env0)
				gomega.Expect(o).To(gomega.BeEquivalentTo(0))

				o = saveEvent(tc.Context, dataStore, env1)
				gomega.Expect(o).To(gomega.BeEquivalentTo(1))
			})

			ginkgo.It("returns the offset of the event for subsequent calls in the same transaction", func() {
				err := persistence.WithTransaction(
					tc.Context,
					dataStore,
					func(tx persistence.ManagedTransaction) error {
						o, err := tx.SaveEvent(tc.Context, env0)
						gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
						gomega.Expect(o).To(gomega.BeEquivalentTo(0))

						o, err = tx.SaveEvent(tc.Context, env1)
						gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
						gomega.Expect(o).To(gomega.BeEquivalentTo(1))

						return nil
					},
				)
				gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
			})

			ginkgo.It("blocks if another in-flight transaction has saved events", func() {
				tx1, err := dataStore.Begin(tc.Context)
				gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
				defer tx1.Rollback()

				_, err = tx1.SaveEvent(tc.Context, env0)
				gomega.Expect(err).ShouldNot(gomega.HaveOccurred())

				tx2, err := dataStore.Begin(tc.Context)
				gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
				defer tx2.Rollback()

				ctx, cancel := context.WithTimeout(tc.Context, 50*time.Millisecond)
				defer cancel()

				_, err = tx2.SaveEvent(ctx, env1)
				gomega.Expect(err).To(gomega.Equal(context.DeadlineExceeded))
			})

			ginkgo.It("serializes operations from competing transactions", func() {
				ginkgo.By("running several transactions in parallel")

				var (
					g      sync.WaitGroup
					m      sync.Mutex
					expect []*eventstore.Item
				)

				fn := func(env *envelopespec.Envelope) {
					defer ginkgo.GinkgoRecover()
					defer g.Done()
					o := saveEvent(tc.Context, dataStore, env)

					m.Lock()
					defer m.Unlock()

					expect = append(
						expect,
						&eventstore.Item{
							Offset:   o,
							Envelope: env,
						},
					)
				}

				g.Add(3)
				go fn(env0)
				go fn(env1)
				go fn(env2)
				g.Wait()

				ginkgo.By("querying the events")

				// Sort the expected slice, as the appends in each goroutine
				// could be out-of-sync with the saves.
				sort.Slice(
					expect,
					func(i, j int) bool {
						return expect[i].Offset < expect[j].Offset
					},
				)

				items := queryEvents(tc.Context, repository, eventstore.Query{})
				expectItemsToEqual(items, expect)
			})

			ginkgo.When("the transaction is rolled-back", func() {
				ginkgo.It("does not save any events", func() {
					err := common.WithTransactionRollback(
						tc.Context,
						dataStore,
						func(tx persistence.ManagedTransaction) error {
							_, err := tx.SaveEvent(tc.Context, env0)
							return err
						},
					)
					gomega.Expect(err).ShouldNot(gomega.HaveOccurred())

					items := queryEvents(tc.Context, repository, eventstore.Query{})
					gomega.Expect(items).To(gomega.BeEmpty())
				})

				ginkgo.It("does not increment the offset", func() {
					err := common.WithTransactionRollback(
						tc.Context,
						dataStore,
						func(tx persistence.ManagedTransaction) error {
							_, err := tx.SaveEvent(tc.Context, env0)
							return err
						},
					)
					gomega.Expect(err).ShouldNot(gomega.HaveOccurred())

					o := saveEvent(tc.Context, dataStore, env0)
					gomega.Expect(o).To(gomega.BeEquivalentTo(0))
				})
			})
		})
	})
}
