package providertest

import (
	"context"
	"time"

	dogmafixtures "github.com/dogmatiq/dogma/fixtures"
	"github.com/dogmatiq/infix/draftspecs/envelopespec"
	infixfixtures "github.com/dogmatiq/infix/fixtures"
	"github.com/dogmatiq/infix/persistence"
	"github.com/dogmatiq/infix/persistence/subsystem/eventstore"
	"github.com/onsi/ginkgo"
	"github.com/onsi/gomega"
	"golang.org/x/sync/errgroup"
)

func declareEventStoreTransactionTests(
	ctx *context.Context,
	in *In,
	out *Out,
) {
	ginkgo.Context("package eventstore", func() {
		var (
			provider      persistence.Provider
			closeProvider func()
			dataStore     persistence.DataStore
			repository    eventstore.Repository

			env0 = infixfixtures.NewEnvelopeProto("<message-0>", dogmafixtures.MessageA1)
			env1 = infixfixtures.NewEnvelopeProto("<message-1>", dogmafixtures.MessageB1)
			env2 = infixfixtures.NewEnvelopeProto("<message-2>", dogmafixtures.MessageC1)
		)

		ginkgo.BeforeEach(func() {
			provider, closeProvider = out.NewProvider()

			var err error
			dataStore, err = provider.Open(*ctx, "<app-key>")
			gomega.Expect(err).ShouldNot(gomega.HaveOccurred())

			repository = dataStore.EventStoreRepository()
		})

		ginkgo.AfterEach(func() {
			if dataStore != nil {
				dataStore.Close()
			}

			if closeProvider != nil {
				closeProvider()
			}
		})

		ginkgo.Describe("type Transaction (interface)", func() {
			ginkgo.Describe("func SaveEvent()", func() {
				ginkgo.It("returns the offset of the event", func() {
					o, err := saveEvent(*ctx, dataStore, env0)
					gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
					gomega.Expect(o).To(gomega.Equal(eventstore.Offset(0)))

					o, err = saveEvent(*ctx, dataStore, env1)
					gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
					gomega.Expect(o).To(gomega.Equal(eventstore.Offset(1)))
				})

				ginkgo.It("returns the offset of the event for subsequent calls in the same transaction", func() {
					err := persistence.WithTransaction(
						*ctx,
						dataStore,
						func(tx persistence.ManagedTransaction) error {
							o, err := tx.SaveEvent(*ctx, env0)
							gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
							gomega.Expect(o).To(gomega.Equal(eventstore.Offset(0)))

							o, err = tx.SaveEvent(*ctx, env1)
							gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
							gomega.Expect(o).To(gomega.Equal(eventstore.Offset(1)))

							return nil
						},
					)
					gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
				})

				ginkgo.It("blocks if another in-flight transaction has saved events", func() {
					tx1, err := dataStore.Begin(*ctx)
					gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
					defer tx1.Rollback()

					_, err = tx1.SaveEvent(*ctx, env0)
					gomega.Expect(err).ShouldNot(gomega.HaveOccurred())

					tx2, err := dataStore.Begin(*ctx)
					gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
					defer tx2.Rollback()

					ctx, cancel := context.WithTimeout(*ctx, 50*time.Millisecond)
					defer cancel()

					_, err = tx2.SaveEvent(ctx, env1)
					gomega.Expect(err).To(gomega.Equal(context.DeadlineExceeded))
				})

				ginkgo.It("serializes operations from competing transactions", func() {
					// Create a slice of envelopes to test with.
					envelopes := []*envelopespec.Envelope{
						env0,
						env1,
						env2,
					}

					// Create a slice to store the offset that each call to
					// SaveEvent() returns.
					offsets := make([]eventstore.Offset, len(envelopes))

					ginkgo.By("running several transactions in parallel")

					g, gctx := errgroup.WithContext(*ctx)
					for i := range envelopes {
						i := i // capture loop variable
						g.Go(func() error {
							var err error
							offsets[i], err = saveEvent(
								gctx,
								dataStore,
								envelopes[i],
							)
							return err
						})
					}

					err := g.Wait()
					gomega.Expect(err).ShouldNot(gomega.HaveOccurred())

					// We don't know what order the transactions will execute,
					// so allow for the offsets to be in any order.
					expected := make([]eventstore.Offset, len(offsets))
					for i := range expected {
						expected[i] = eventstore.Offset(i)
					}
					gomega.Expect(offsets).To(
						gomega.ConsistOf(expected),
						"unexpected offsets were returned",
					)

					ginkgo.By("querying the events")

					// Now we query the events and verify that each specific
					// envelope ended up at the offset that reported to us.
					events, err := queryEvents(*ctx, repository, eventstore.Query{})
					gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
					gomega.Expect(len(events)).To(
						gomega.Equal(len(envelopes)),
						"the number of events saved to the store is incorrect",
					)

					// i is the "envelope number" (envN), o is the offset we
					// expect that event to be at.
					for i, o := range offsets {
						ev := events[o]
						env := envelopes[i]

						gomega.Expect(ev.ID()).To(
							gomega.Equal(env.MetaData.MessageId),
						)
					}
				})

				ginkgo.When("the transaction is rolled-back", func() {
					ginkgo.BeforeEach(func() {
						tx, err := dataStore.Begin(*ctx)
						gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
						defer tx.Rollback()

						_, err = tx.SaveEvent(*ctx, env0)
						gomega.Expect(err).ShouldNot(gomega.HaveOccurred())

						err = tx.Rollback()
						gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
					})

					ginkgo.It("does not save any events", func() {
						events, err := queryEvents(*ctx, repository, eventstore.Query{})
						gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
						gomega.Expect(events).To(gomega.BeEmpty())
					})

					ginkgo.It("does not increment the offset", func() {
						o, err := saveEvent(*ctx, dataStore, env0)
						gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
						gomega.Expect(o).To(gomega.Equal(eventstore.Offset(0)))
					})
				})
			})
		})
	})
}
