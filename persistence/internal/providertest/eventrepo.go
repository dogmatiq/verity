package providertest

import (
	"context"
	"sync"

	dogmafixtures "github.com/dogmatiq/dogma/fixtures"
	infixfixtures "github.com/dogmatiq/infix/fixtures"
	"github.com/dogmatiq/infix/persistence"
	"github.com/dogmatiq/infix/persistence/subsystem/eventstore"
	marshalfixtures "github.com/dogmatiq/marshalkit/fixtures"
	"github.com/jmalloc/gomegax"
	"github.com/onsi/ginkgo"
	"github.com/onsi/gomega"
)

// declareEventRepositoryTests declares a functional test-suite for a specific
// eventstore.Repository implementation.
func declareEventRepositoryTests(tc *TestContext) {
	ginkgo.Describe("type eventstore.Repository", func() {
		var (
			dataStore persistence.DataStore
			tearDown  func()

			item0, item1, item2, item3, item4, item5 eventstore.Item
			filter                                   map[string]struct{}
		)

		ginkgo.BeforeEach(func() {
			dataStore, tearDown = tc.SetupDataStore()

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

			filter = map[string]struct{}{
				item0.Envelope.PortableName: {},
				item1.Envelope.PortableName: {},
				item2.Envelope.PortableName: {},
				item3.Envelope.PortableName: {},
				item4.Envelope.PortableName: {},
				item5.Envelope.PortableName: {},
			}
		})

		ginkgo.AfterEach(func() {
			tearDown()
		})

		ginkgo.When("the store is empty", func() {
			ginkgo.Describe("func NextEventOffset()", func() {
				ginkgo.It("returns zero", func() {
					o, err := dataStore.NextEventOffset(tc.Context)
					gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
					gomega.Expect(o).To(gomega.BeEquivalentTo(0))
				})
			})

			ginkgo.Describe("func LoadEventsByType()", func() {
				ginkgo.It("returns an empty result", func() {
					items := loadEventsByType(
						tc.Context,
						dataStore,
						filter,
						0,
					)
					gomega.Expect(items).To(gomega.BeEmpty())
				})
			})

			ginkgo.Describe("func LoadEventsBySource()", func() {
				ginkgo.It("returns an empty result", func() {
					items := loadEventsBySource(
						tc.Context,
						dataStore,
						"<aggregate>",
						"<instance-a>",
						"",
					)
					gomega.Expect(items).To(gomega.BeEmpty())
				})
			})
		})

		ginkgo.When("the store is not empty", func() {
			ginkgo.BeforeEach(func() {
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
			})

			ginkgo.Describe("func NextEventOffset()", func() {
				ginkgo.It("returns the offset after the last recorded event", func() {
					o, err := dataStore.NextEventOffset(tc.Context)
					gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
					gomega.Expect(o).To(gomega.BeEquivalentTo(6))
				})

				ginkgo.It("does not block if the context is canceled", func() {
					// This test ensures that the implementation returns
					// immediately, either with a context.Canceled error, or
					// with the correct result.
					ctx, cancel := context.WithCancel(tc.Context)
					cancel()

					o, err := dataStore.NextEventOffset(ctx)
					if err != nil {
						gomega.Expect(err).To(gomega.Equal(context.Canceled))
					} else {
						gomega.Expect(o).To(gomega.BeEquivalentTo(6))
					}
				})
			})

			ginkgo.Describe("func LoadEventsByType()", func() {
				ginkgo.It("returns events that match the type filter", func() {
					items := loadEventsByType(
						tc.Context,
						dataStore,
						map[string]struct{}{
							marshalfixtures.MessageAPortableName: {},
							marshalfixtures.MessageCPortableName: {},
						},
						0,
					)

					gomega.Expect(items).To(gomegax.EqualX(
						[]eventstore.Item{
							item0,
							item2,
							item3,
							item5,
						},
					))
				})

				ginkgo.It("returns events at or after the minimum offset", func() {
					items := loadEventsByType(
						tc.Context,
						dataStore,
						filter,
						2,
					)

					gomega.Expect(items).To(gomegax.EqualX(
						[]eventstore.Item{
							item2,
							item3,
							item4,
							item5,
						},
					))
				})

				ginkgo.It("returns an empty result if the minimum offset is larger than the largest offset", func() {
					items := loadEventsByType(
						tc.Context,
						dataStore,
						filter,
						100,
					)

					gomega.Expect(items).To(gomega.BeEmpty())
				})

				ginkgo.It("correctly applies filters in concurrently accessed results", func() {
					var g sync.WaitGroup

					fn := func() {
						defer g.Done()
						defer ginkgo.GinkgoRecover()

						items := loadEventsByType(
							tc.Context,
							dataStore,
							map[string]struct{}{
								marshalfixtures.MessageAPortableName: {},
							},
							0,
						)
						gomega.Expect(items).To(gomegax.EqualX(
							[]eventstore.Item{
								item0,
								item3,
							},
						))
					}

					g.Add(3)
					go fn()
					go fn()
					go fn()
					g.Wait()
				})
			})

			ginkgo.Describe("func LoadEventsBySource()", func() {
				ginkgo.It("returns the events produced by the given source instance", func() {
					items := loadEventsBySource(
						tc.Context,
						dataStore,
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
				})

				ginkgo.It("only returns events recorded after the barrier message", func() {
					items := loadEventsBySource(
						tc.Context,
						dataStore,
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
					_, err := dataStore.LoadEventsBySource(
						tc.Context,
						"<aggregate>",
						"<instance-a>",
						"<unknown>",
					)
					gomega.Expect(err).Should(gomega.HaveOccurred())
					gomega.Expect(err).To(gomega.Equal(
						persistence.UnknownMessageError{
							MessageID: "<unknown>",
						},
					))
				})
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

					res, err := dataStore.LoadEventsByType(tc.Context, filter, 0)
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
					res, err := dataStore.LoadEventsByType(tc.Context, filter, 0)
					gomega.Expect(err).ShouldNot(gomega.HaveOccurred())

					err = res.Close()
					gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
				})

				ginkgo.It("does not panic if the result is already closed", func() {
					// The return value of res.Close() for a result that is
					// already closed is implementation defined, so this test
					// simply verifies that it returns *something* without
					// panicking.
					res, err := dataStore.LoadEventsByType(tc.Context, filter, 0)
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
