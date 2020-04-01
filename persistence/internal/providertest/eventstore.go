package providertest

import (
	"context"
	"fmt"
	"sync"
	"time"

	dogmafixtures "github.com/dogmatiq/dogma/fixtures"
	"github.com/dogmatiq/infix/draftspecs/envelopespec"
	infixfixtures "github.com/dogmatiq/infix/fixtures"
	"github.com/dogmatiq/infix/persistence"
	"github.com/dogmatiq/infix/persistence/subsystem/eventstore"
	marshalfixtures "github.com/dogmatiq/marshalkit/fixtures"
	"github.com/golang/protobuf/proto"
	"github.com/onsi/ginkgo"
	"github.com/onsi/ginkgo/extensions/table"
	"github.com/onsi/gomega"
	"golang.org/x/sync/errgroup"
)

func declareEventStoreTests(
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
			env3 = infixfixtures.NewEnvelopeProto("<message-3>", dogmafixtures.MessageA2)
			env4 = infixfixtures.NewEnvelopeProto("<message-4>", dogmafixtures.MessageB2)
			env5 = infixfixtures.NewEnvelopeProto("<message-5>", dogmafixtures.MessageC2)

			event0 = &eventstore.Event{Offset: 0, Envelope: env0}
			event1 = &eventstore.Event{Offset: 1, Envelope: env1}
			event2 = &eventstore.Event{Offset: 2, Envelope: env2}
			event3 = &eventstore.Event{Offset: 3, Envelope: env3}
			event4 = &eventstore.Event{Offset: 4, Envelope: env4}
			event5 = &eventstore.Event{Offset: 5, Envelope: env5}
		)

		// Setup some different source handler values to test the aggregate
		// instance filtering.
		env0.MetaData.Source.Handler.Key = "<aggregate>"
		env0.MetaData.Source.InstanceId = "<instance-a>"

		env1.MetaData.Source.Handler.Key = "<aggregate>"
		env1.MetaData.Source.InstanceId = "<instance-b>"

		env2.MetaData.Source.Handler.Key = "<aggregate>"
		env2.MetaData.Source.InstanceId = "<instance-a>"

		env3.MetaData.Source.Handler.Key = "<aggregate>"
		env3.MetaData.Source.InstanceId = "<instance-b>"

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
						func(tx persistence.Transaction) error {
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

						gomega.Expect(ev.Envelope.MetaData.MessageId).To(
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

		ginkgo.Describe("type Repository (interface)", func() {
			ginkgo.Describe("func QueryEvents()", func() {
				ginkgo.It("returns an empty result if the store is empty", func() {
					res, err := repository.QueryEvents(*ctx, eventstore.Query{})
					gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
					defer res.Close()

					_, ok, err := res.Next(*ctx)
					gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
					gomega.Expect(ok).To(gomega.BeFalse())
				})

				table.DescribeTable(
					"it returns a result containing the events that match the query criteria",
					func(q eventstore.Query, expected ...*eventstore.Event) {
						ginkgo.By("saving some events")

						err := saveEvents(
							*ctx,
							dataStore,
							env0,
							env1,
							env2,
							env3,
							env4,
							env5,
						)
						gomega.Expect(err).ShouldNot(gomega.HaveOccurred())

						ginkgo.By("querying the events")

						res, err := repository.QueryEvents(*ctx, q)
						gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
						defer res.Close()

						ginkgo.By("iterating through the result")

						count := len(expected)
						index := 0

						for {
							ev, ok, err := res.Next(*ctx)
							gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
							if !ok {
								break
							}

							gomega.Expect(index).To(
								gomega.BeNumerically("<", count),
								"too many events included in the result",
							)

							x := expected[index]

							gomega.Expect(ev.Offset).To(
								gomega.Equal(x.Offset),
								fmt.Sprintf("event at index #%d in result has the wrong offset", index),
							)

							if !proto.Equal(ev.Envelope, x.Envelope) {
								gomega.Expect(ev.Envelope).To(
									gomega.Equal(x.Envelope),
									fmt.Sprintf("event at index #%d in result does not match the expected envelope", index),
								)
							}

							index++
						}

						gomega.Expect(index).To(
							gomega.Equal(count),
							"too few events included in the result",
						)
					},
					table.Entry(
						"it includes all events by default",
						eventstore.Query{},
						event0, event1, event2, event3, event4, event5,
					),
					table.Entry(
						"it honours the minimum offset",
						eventstore.Query{MinOffset: 3},
						event3, event4, event5,
					),
					table.Entry(
						"it returns an empty result if the minimum offset is larger than the largest offset",
						eventstore.Query{MinOffset: 100},
					),
					table.Entry(
						"it honours the type filter",
						eventstore.Query{
							Filter: eventstore.NewFilter(
								marshalfixtures.MessageAPortableName,
								marshalfixtures.MessageCPortableName,
							),
						},
						event0, event2, event3, event5,
					),
					table.Entry(
						"it honours the aggregate instance filter",
						eventstore.Query{
							AggregateHandlerKey: "<aggregate>",
							AggregateInstanceID: "<instance-a>",
						},
						event0, event2,
					),
				)

				ginkgo.It("allows concurrent filtered consumers for the same application", func() {
					err := saveEvents(
						*ctx,
						dataStore,
						env0,
						env1,
						env2,
						env3,
						env4,
						env5,
					)
					gomega.Expect(err).ShouldNot(gomega.HaveOccurred())

					q := eventstore.Query{
						Filter: eventstore.NewFilter(
							marshalfixtures.MessageAPortableName,
						),
					}

					var g sync.WaitGroup

					fn := func() {
						defer g.Done()
						defer ginkgo.GinkgoRecover()
						events, err := queryEvents(*ctx, repository, q)
						gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
						gomega.Expect(events).To(gomega.HaveLen(2))
					}

					g.Add(3)

					go fn()
					go fn()
					go fn()

					g.Wait()
				})

				ginkgo.It("does not return an error if events exist beyond the end offset", func() {
					ginkgo.By("querying the events")

					res, err := repository.QueryEvents(*ctx, eventstore.Query{})
					gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
					defer res.Close()

					ginkgo.By("saving some events")

					err = saveEvents(
						*ctx,
						dataStore,
						env0,
						env1,
					)
					gomega.Expect(err).ShouldNot(gomega.HaveOccurred())

					ginkgo.By("iterating through the result")

					// The implementation may or may not expose these newly
					// appended events to the caller. We simply want to ensure
					// that no error occurs.
					_, _, err = res.Next(*ctx)
					gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
				})
			})
		})

		ginkgo.Describe("type Result (interface)", func() {
			ginkgo.Describe("func Next()", func() {
				ginkgo.It("returns an error if the context is canceled", func() {
					res, err := repository.QueryEvents(*ctx, eventstore.Query{})
					gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
					defer res.Close()

					ctx, cancel := context.WithCancel(*ctx)
					cancel()

					_, _, err = res.Next(ctx)
					gomega.Expect(err).To(gomega.Equal(context.Canceled))
				})
			})

			ginkgo.Describe("func Close()", func() {
				ginkgo.It("does not return an error if the result is open", func() {
					res, err := repository.QueryEvents(*ctx, eventstore.Query{
						MinOffset: 3,
						Filter: eventstore.NewFilter(
							marshalfixtures.MessageAPortableName,
							marshalfixtures.MessageCPortableName,
						),
						AggregateHandlerKey: "<aggregate>",
						AggregateInstanceID: "<instance-a>",
					})
					gomega.Expect(err).ShouldNot(gomega.HaveOccurred())

					err = res.Close()
					gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
				})

				ginkgo.It("does not panic if the result is already closed", func() {
					// The return value of res.Close() for a result that is
					// already closed is implementation defined, so this test
					// simply verifies that it returns *something* without
					// panicking.
					res, err := repository.QueryEvents(*ctx, eventstore.Query{})
					gomega.Expect(err).ShouldNot(gomega.HaveOccurred())

					res.Close()

					gomega.Expect(func() {
						res.Close()
					}).NotTo(gomega.Panic())
				})
			})
		})
	})
}
