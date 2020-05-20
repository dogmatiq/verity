package providertest

import (
	"context"
	"sync"

	dogmafixtures "github.com/dogmatiq/dogma/fixtures"
	"github.com/dogmatiq/infix/draftspecs/envelopespec"
	infixfixtures "github.com/dogmatiq/infix/fixtures"
	"github.com/dogmatiq/infix/persistence"
	"github.com/dogmatiq/infix/persistence/subsystem/eventstore"
	marshalfixtures "github.com/dogmatiq/marshalkit/fixtures"
	"github.com/jmalloc/gomegax"
	"github.com/onsi/ginkgo"
	"github.com/onsi/ginkgo/extensions/table"
	"github.com/onsi/gomega"
)

// declareEventRepositoryTests declares a functional test-suite for a specific
// eventstore.Repository implementation.
func declareEventRepositoryTests(tc *TestContext) {
	ginkgo.Describe("type eventstore.Repository", func() {
		var (
			dataStore  persistence.DataStore
			repository eventstore.Repository
			tearDown   func()

			item0, item1, item2, item3, item4, item5 eventstore.Item
		)

		ginkgo.BeforeEach(func() {
			dataStore, tearDown = tc.SetupDataStore()
			repository = dataStore.EventStoreRepository()

			item0 = eventstore.Item{
				Offset:   0,
				Envelope: infixfixtures.NewEnvelope("<message-0>", dogmafixtures.MessageA1),
			}

			item1 = eventstore.Item{
				Offset:   1,
				Envelope: infixfixtures.NewEnvelope("<message-1>", dogmafixtures.MessageB1),
			}

			item2 = eventstore.Item{
				Offset:   2,
				Envelope: infixfixtures.NewEnvelope("<message-2>", dogmafixtures.MessageC1),
			}

			item3 = eventstore.Item{
				Offset:   3,
				Envelope: infixfixtures.NewEnvelope("<message-3>", dogmafixtures.MessageA2),
			}

			item4 = eventstore.Item{
				Offset:   4,
				Envelope: infixfixtures.NewEnvelope("<message-4>", dogmafixtures.MessageB2),
			}

			item5 = eventstore.Item{
				Offset:   5,
				Envelope: infixfixtures.NewEnvelope("<message-5>", dogmafixtures.MessageC2),
			}

			// Setup some different source handler values to test the aggregate
			// instance filtering.
			item0.Envelope.MetaData.Source.Handler.Key = "<aggregate>"
			item0.Envelope.MetaData.Source.InstanceId = "<instance-a>"

			item1.Envelope.MetaData.Source.Handler.Key = "<aggregate>"
			item1.Envelope.MetaData.Source.InstanceId = "<instance-b>"

			item2.Envelope.MetaData.Source.Handler.Key = "<aggregate>"
			item2.Envelope.MetaData.Source.InstanceId = "<instance-a>"

			item3.Envelope.MetaData.Source.Handler.Key = "<aggregate>"
			item3.Envelope.MetaData.Source.InstanceId = "<instance-b>"
		})

		ginkgo.AfterEach(func() {
			tearDown()
		})

		ginkgo.Describe("func NextEventOffset()", func() {
			ginkgo.It("returns zero if the store is empty", func() {
				o, err := repository.NextEventOffset(tc.Context)
				gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
				gomega.Expect(o).To(gomega.BeEquivalentTo(0))
			})

			ginkgo.It("returns the offset after the last recorded event", func() {
				var g sync.WaitGroup

				fn := func(env *envelopespec.Envelope) {
					defer ginkgo.GinkgoRecover()
					defer g.Done()
					persist(
						tc.Context,
						dataStore,
						persistence.SaveEvent{
							Envelope: env,
						},
					)
				}

				g.Add(3)
				go fn(item0.Envelope)
				go fn(item1.Envelope)
				go fn(item2.Envelope)
				g.Wait()

				o, err := repository.NextEventOffset(tc.Context)
				gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
				gomega.Expect(o).To(gomega.BeEquivalentTo(3))
			})

			ginkgo.It("does not block if the context is canceled", func() {
				// This test ensures that the implementation returns
				// immediately, either with a context.Canceled error, or with
				// the correct result.
				persist(
					tc.Context,
					dataStore,
					persistence.SaveEvent{
						Envelope: item0.Envelope,
					},
				)

				ctx, cancel := context.WithCancel(tc.Context)
				cancel()

				o, err := repository.NextEventOffset(ctx)
				if err != nil {
					gomega.Expect(err).To(gomega.Equal(context.Canceled))
				} else {
					gomega.Expect(o).To(gomega.BeEquivalentTo(1))
				}
			})
		})

		ginkgo.Describe("func QueryEvents()", func() {
			ginkgo.It("returns an empty result if the store is empty", func() {
				items := queryEvents(tc.Context, repository, eventstore.Query{})
				gomega.Expect(items).To(gomega.BeEmpty())
			})

			table.DescribeTable(
				"it returns a result containing the events that match the query criteria",
				func(q eventstore.Query, pointers ...*eventstore.Item) {
					persist(
						tc.Context,
						dataStore,
						persistence.SaveEvent{
							Envelope: item0.Envelope,
						},
						persistence.SaveEvent{
							Envelope: item1.Envelope,
						},
						persistence.SaveEvent{
							Envelope: item2.Envelope,
						},
						persistence.SaveEvent{
							Envelope: item3.Envelope,
						},
						persistence.SaveEvent{
							Envelope: item4.Envelope,
						},
						persistence.SaveEvent{
							Envelope: item5.Envelope,
						},
					)

					var expected []eventstore.Item
					for _, p := range pointers {
						expected = append(expected, *p)
					}

					items := queryEvents(tc.Context, repository, q)
					gomega.Expect(items).To(gomegax.EqualX(expected))
				},
				table.Entry(
					"it includes all events by default",
					eventstore.Query{},
					&item0, &item1, &item2, &item3, &item4, &item5,
				),
				table.Entry(
					"it honours the minimum offset",
					eventstore.Query{MinOffset: 3},
					&item3, &item4, &item5,
				),
				table.Entry(
					"it returns an empty result if the minimum offset is larger than the largest offset",
					eventstore.Query{MinOffset: 100},
					// no results
				),
				table.Entry(
					"it honours the type filter",
					eventstore.Query{
						Filter: eventstore.NewFilter(
							marshalfixtures.MessageAPortableName,
							marshalfixtures.MessageCPortableName,
						),
					},
					&item0, &item2, &item3, &item5,
				),
				table.Entry(
					"it honours the aggregate instance filter",
					eventstore.Query{
						AggregateHandlerKey: "<aggregate>",
						AggregateInstanceID: "<instance-a>",
					},
					&item0, &item2,
				),
			)

			ginkgo.It("does not return an error if events exist beyond the end offset", func() {
				res, err := repository.QueryEvents(tc.Context, eventstore.Query{})
				gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
				defer res.Close()

				persist(
					tc.Context,
					dataStore,
					persistence.SaveEvent{
						Envelope: item0.Envelope,
					},
					persistence.SaveEvent{
						Envelope: item1.Envelope,
					},
				)

				// The implementation may or may not expose these newly
				// appended events to the caller. We simply want to ensure
				// that no error occurs.
				_, _, err = res.Next(tc.Context)
				gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
			})
		})

		ginkgo.Describe("func LoadEventsBySource()", func() {
			ginkgo.It("returns an empty result if the store is empty", func() {
				items := loadEventsBySource(tc.Context, repository, "<aggregate>", "<instance-a>", "")
				gomega.Expect(items).To(gomega.BeEmpty())
			})

			ginkgo.It("returns a result containing the events that match the source instance", func() {
				persist(
					tc.Context,
					dataStore,
					persistence.SaveEvent{
						Envelope: item0.Envelope,
					},
					persistence.SaveEvent{
						Envelope: item1.Envelope,
					},
					persistence.SaveEvent{
						Envelope: item2.Envelope,
					},
					persistence.SaveEvent{
						Envelope: item3.Envelope,
					},
					persistence.SaveEvent{
						Envelope: item4.Envelope,
					},
					persistence.SaveEvent{
						Envelope: item5.Envelope,
					},
				)

				items := loadEventsBySource(
					tc.Context,
					repository,
					"<aggregate>",
					"<instance-a>",
					"",
				)
				gomega.Expect(items).To(gomegax.EqualX(
					[]eventstore.Item{
						item0,
						item2,
					},
				))

				items = loadEventsBySource(
					tc.Context,
					repository,
					"<aggregate>",
					"<instance-b>",
					"",
				)
				gomega.Expect(items).To(gomegax.EqualX(
					[]eventstore.Item{
						item1,
						item3,
					},
				))
			})

			ginkgo.It("only matches the events after the barrier message", func() {
				persist(
					tc.Context,
					dataStore,
					persistence.SaveEvent{
						Envelope: item0.Envelope,
					},
					persistence.SaveEvent{
						Envelope: item1.Envelope,
					},
					persistence.SaveEvent{
						Envelope: item2.Envelope,
					},
				)

				items := loadEventsBySource(
					tc.Context,
					repository,
					"<aggregate>",
					"<instance-a>",
					item0.ID(),
				)
				gomega.Expect(items).To(gomegax.EqualX(
					[]eventstore.Item{
						item2,
					},
				))
			})

			ginkgo.It("returns an error if the barrier message is not found", func() {
				persist(
					tc.Context,
					dataStore,
					persistence.SaveEvent{
						Envelope: item0.Envelope,
					},
					persistence.SaveEvent{
						Envelope: item1.Envelope,
					},
				)

				_, err := repository.LoadEventsBySource(
					tc.Context,
					"<aggregate>",
					"<instance-a>",
					"<unknown>",
				)
				gomega.Expect(err).Should(gomega.HaveOccurred())
				gomega.Expect(err).To(
					gomega.MatchError("message with ID '<unknown>' cannot be found"),
				)
			})

			ginkgo.It("does not return an error if events exist beyond the end offset", func() {
				res, err := repository.LoadEventsBySource(tc.Context, "<aggregate>", "<instance-a>", "")
				gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
				defer res.Close()

				persist(
					tc.Context,
					dataStore,
					persistence.SaveEvent{
						Envelope: item0.Envelope,
					},
					persistence.SaveEvent{
						Envelope: item1.Envelope,
					},
				)

				// The implementation may or may not expose these newly
				// appended events to the caller. We simply want to ensure
				// that no error occurs.
				_, _, err = res.Next(tc.Context)
				gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
			})
		})

		ginkgo.Describe("func QueryEvents() and LoadEventsBySource()", func() {
			ginkgo.It("allows concurrent consumers for the same application", func() {
				persist(
					tc.Context,
					dataStore,
					persistence.SaveEvent{
						Envelope: item0.Envelope,
					},
					persistence.SaveEvent{
						Envelope: item1.Envelope,
					},
					persistence.SaveEvent{
						Envelope: item2.Envelope,
					},
					persistence.SaveEvent{
						Envelope: item3.Envelope,
					},
					persistence.SaveEvent{
						Envelope: item4.Envelope,
					},
					persistence.SaveEvent{
						Envelope: item5.Envelope,
					},
				)

				q := eventstore.Query{
					Filter: eventstore.NewFilter(
						marshalfixtures.MessageAPortableName,
					),
				}

				var g sync.WaitGroup

				fn1 := func() {
					defer g.Done()
					defer ginkgo.GinkgoRecover()
					items := queryEvents(tc.Context, repository, q)
					gomega.Expect(items).To(gomega.HaveLen(2))
				}

				fn2 := func() {
					defer g.Done()
					defer ginkgo.GinkgoRecover()
					items := loadEventsBySource(
						tc.Context,
						repository,
						"<aggregate>",
						"<instance-a>",
						"",
					)
					gomega.Expect(items).To(gomega.HaveLen(2))
				}

				g.Add(6)
				go fn1()
				go fn2()
				go fn1()
				go fn2()
				go fn1()
				go fn2()
				g.Wait()
			})
		})

		ginkgo.Describe("type eventstore.Result", func() {
			ginkgo.Describe("func Next()", func() {
				ginkgo.It("returns an error if the context is canceled", func() {
					// This test ensures that the implementation returns
					// immediately, either with a context.Canceled error, or
					// with the correct result.

					persist(
						tc.Context,
						dataStore,
						persistence.SaveEvent{
							Envelope: item0.Envelope,
						},
					)

					res, err := repository.QueryEvents(tc.Context, eventstore.Query{})
					gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
					defer res.Close()

					ctx, cancel := context.WithCancel(tc.Context)
					cancel()

					item, ok, err := res.Next(ctx)
					if err != nil {
						gomega.Expect(err).To(gomega.Equal(context.Canceled))
					} else {
						gomega.Expect(item).To(gomegax.EqualX(&item0))
						gomega.Expect(ok).To(gomega.BeTrue())
					}
				})
			})

			ginkgo.Describe("func Close()", func() {
				ginkgo.It("does not return an error if the result is open", func() {
					res, err := repository.QueryEvents(tc.Context, eventstore.Query{
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
					res, err := repository.QueryEvents(tc.Context, eventstore.Query{})
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
