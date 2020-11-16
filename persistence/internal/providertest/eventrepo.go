package providertest

import (
	"context"
	"sync"

	dogmafixtures "github.com/dogmatiq/dogma/fixtures"
	verityfixtures "github.com/dogmatiq/verity/fixtures"
	"github.com/dogmatiq/verity/persistence"
	marshalfixtures "github.com/dogmatiq/marshalkit/fixtures"
	"github.com/jmalloc/gomegax"
	"github.com/onsi/ginkgo"
	"github.com/onsi/gomega"
)

// declareEventRepositoryTests declares a functional test-suite for a specific
// persistence.EventRepository implementation.
func declareEventRepositoryTests(tc *TestContext) {
	ginkgo.Describe("type persistence.EventRepository", func() {
		var (
			dataStore persistence.DataStore
			tearDown  func()

			ev0, ev1, ev2, ev3, ev4, ev5 persistence.Event
			filter                       map[string]struct{}
		)

		ginkgo.BeforeEach(func() {
			dataStore, tearDown = tc.SetupDataStore()

			ev0 = persistence.Event{
				Offset:   0,
				Envelope: verityfixtures.NewEnvelope("<message-0>", dogmafixtures.MessageA1),
			}

			ev1 = persistence.Event{
				Offset:   1,
				Envelope: verityfixtures.NewEnvelope("<message-1>", dogmafixtures.MessageB1),
			}

			ev2 = persistence.Event{
				Offset:   2,
				Envelope: verityfixtures.NewEnvelope("<message-2>", dogmafixtures.MessageC1),
			}

			ev3 = persistence.Event{
				Offset:   3,
				Envelope: verityfixtures.NewEnvelope("<message-3>", dogmafixtures.MessageA2),
			}

			ev4 = persistence.Event{
				Offset:   4,
				Envelope: verityfixtures.NewEnvelope("<message-4>", dogmafixtures.MessageB2),
			}

			ev5 = persistence.Event{
				Offset:   5,
				Envelope: verityfixtures.NewEnvelope("<message-5>", dogmafixtures.MessageC2),
			}

			// Setup some different source handler values to test the aggregate
			// instance filtering.
			ev0.Envelope.SourceHandler.Key = "<aggregate>"
			ev0.Envelope.SourceInstanceId = "<instance-a>"

			ev1.Envelope.SourceHandler.Key = "<aggregate>"
			ev1.Envelope.SourceInstanceId = "<instance-b>"

			ev2.Envelope.SourceHandler.Key = "<aggregate>"
			ev2.Envelope.SourceInstanceId = "<instance-a>"

			ev3.Envelope.SourceHandler.Key = "<aggregate>"
			ev3.Envelope.SourceInstanceId = "<instance-b>"

			filter = map[string]struct{}{
				ev0.Envelope.PortableName: {},
				ev1.Envelope.PortableName: {},
				ev2.Envelope.PortableName: {},
				ev3.Envelope.PortableName: {},
				ev4.Envelope.PortableName: {},
				ev5.Envelope.PortableName: {},
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
					events := loadEventsByType(
						tc.Context,
						dataStore,
						filter,
						0,
					)
					gomega.Expect(events).To(gomega.BeEmpty())
				})
			})

			ginkgo.Describe("func LoadEventsBySource()", func() {
				ginkgo.It("returns an empty result", func() {
					events := loadEventsBySource(
						tc.Context,
						dataStore,
						"<aggregate>",
						"<instance-a>",
						"",
					)
					gomega.Expect(events).To(gomega.BeEmpty())
				})
			})
		})

		ginkgo.When("the store is not empty", func() {
			ginkgo.BeforeEach(func() {
				persist(
					tc.Context,
					dataStore,
					persistence.SaveEvent{
						Envelope: ev0.Envelope,
					},
					persistence.SaveEvent{
						Envelope: ev1.Envelope,
					},
					persistence.SaveEvent{
						Envelope: ev2.Envelope,
					},
					persistence.SaveEvent{
						Envelope: ev3.Envelope,
					},
					persistence.SaveEvent{
						Envelope: ev4.Envelope,
					},
					persistence.SaveEvent{
						Envelope: ev5.Envelope,
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
					events := loadEventsByType(
						tc.Context,
						dataStore,
						map[string]struct{}{
							marshalfixtures.MessageAPortableName: {},
							marshalfixtures.MessageCPortableName: {},
						},
						0,
					)

					gomega.Expect(events).To(gomegax.EqualX(
						[]persistence.Event{
							ev0,
							ev2,
							ev3,
							ev5,
						},
					))
				})

				ginkgo.It("returns events at or after the minimum offset", func() {
					events := loadEventsByType(
						tc.Context,
						dataStore,
						filter,
						2,
					)

					gomega.Expect(events).To(gomegax.EqualX(
						[]persistence.Event{
							ev2,
							ev3,
							ev4,
							ev5,
						},
					))
				})

				ginkgo.It("returns an empty result if the minimum offset is larger than the largest offset", func() {
					events := loadEventsByType(
						tc.Context,
						dataStore,
						filter,
						100,
					)

					gomega.Expect(events).To(gomega.BeEmpty())
				})

				ginkgo.It("correctly applies filters in concurrently accessed results", func() {
					var g sync.WaitGroup

					fn := func() {
						defer g.Done()
						defer ginkgo.GinkgoRecover()

						events := loadEventsByType(
							tc.Context,
							dataStore,
							map[string]struct{}{
								marshalfixtures.MessageAPortableName: {},
							},
							0,
						)
						gomega.Expect(events).To(gomegax.EqualX(
							[]persistence.Event{
								ev0,
								ev3,
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
					events := loadEventsBySource(
						tc.Context,
						dataStore,
						"<aggregate>",
						"<instance-a>",
						"",
					)
					gomega.Expect(events).To(gomegax.EqualX(
						[]persistence.Event{
							ev0,
							ev2,
						},
					))
				})

				ginkgo.It("only returns events recorded after the barrier message", func() {
					events := loadEventsBySource(
						tc.Context,
						dataStore,
						"<aggregate>",
						"<instance-a>",
						ev0.ID(),
					)
					gomega.Expect(events).To(gomegax.EqualX(
						[]persistence.Event{
							ev2,
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

		ginkgo.Describe("type persistence.EventResult", func() {
			ginkgo.Describe("func Next()", func() {
				ginkgo.It("returns an error if the context is canceled", func() {
					// This test ensures that the implementation returns
					// immediately, either with a context.Canceled error, or
					// with the correct result.

					persist(
						tc.Context,
						dataStore,
						persistence.SaveEvent{
							Envelope: ev0.Envelope,
						},
					)

					res, err := dataStore.LoadEventsByType(tc.Context, filter, 0)
					gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
					defer res.Close()

					ctx, cancel := context.WithCancel(tc.Context)
					cancel()

					ev, ok, err := res.Next(ctx)
					if err != nil {
						gomega.Expect(err).To(gomega.Equal(context.Canceled))
					} else {
						gomega.Expect(ev).To(gomegax.EqualX(ev0))
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
