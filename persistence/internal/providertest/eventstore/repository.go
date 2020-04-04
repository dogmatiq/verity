package eventstore

import (
	"context"
	"fmt"
	"sync"

	dogmafixtures "github.com/dogmatiq/dogma/fixtures"
	"github.com/dogmatiq/infix/draftspecs/envelopespec"
	infixfixtures "github.com/dogmatiq/infix/fixtures"
	"github.com/dogmatiq/infix/persistence"
	"github.com/dogmatiq/infix/persistence/internal/providertest/common"
	"github.com/dogmatiq/infix/persistence/subsystem/eventstore"
	marshalfixtures "github.com/dogmatiq/marshalkit/fixtures"
	"github.com/onsi/ginkgo"
	"github.com/onsi/ginkgo/extensions/table"
	"github.com/onsi/gomega"
)

// DeclareRepositoryTests declares a functional test-suite for a specific
// eventstore.Repository implementation.
func DeclareRepositoryTests(tc *common.TestContext) {
	ginkgo.Describe("type eventstore.Repository", func() {
		var (
			dataStore  persistence.DataStore
			repository eventstore.Repository
			tearDown   func()

			env0, env1, env2, env3, env4, env5             *envelopespec.Envelope
			event0, event1, event2, event3, event4, event5 *eventstore.Event
		)

		ginkgo.BeforeEach(func() {
			dataStore, tearDown = tc.SetupDataStore()
			repository = dataStore.EventStoreRepository()

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
		})

		ginkgo.AfterEach(func() {
			tearDown()
		})

		ginkgo.Describe("func QueryEvents()", func() {
			ginkgo.It("returns an empty result if the store is empty", func() {
				events := queryEvents(tc.Context, repository, eventstore.Query{})
				gomega.Expect(events).To(gomega.BeEmpty())
			})

			table.DescribeTable(
				"it returns a result containing the events that match the query criteria",
				func(q eventstore.Query, expected ...**eventstore.Event) {
					saveEvents(
						tc.Context,
						dataStore,
						env0,
						env1,
						env2,
						env3,
						env4,
						env5,
					)

					events := queryEvents(tc.Context, repository, q)
					gomega.Expect(events).To(gomega.HaveLen(len(expected)))

					for i, ev := range events {
						expectEventToEqual(
							ev,
							*expected[i],
							fmt.Sprintf("event at index #%d of slice", i),
						)
					}
				},
				table.Entry(
					"it includes all events by default",
					eventstore.Query{},
					&event0, &event1, &event2, &event3, &event4, &event5,
				),
				table.Entry(
					"it honours the minimum offset",
					eventstore.Query{MinOffset: 3},
					&event3, &event4, &event5,
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
					&event0, &event2, &event3, &event5,
				),
				table.Entry(
					"it honours the aggregate instance filter",
					eventstore.Query{
						AggregateHandlerKey: "<aggregate>",
						AggregateInstanceID: "<instance-a>",
					},
					&event0, &event2,
				),
			)

			ginkgo.It("allows concurrent filtered consumers for the same application", func() {
				saveEvents(
					tc.Context,
					dataStore,
					env0,
					env1,
					env2,
					env3,
					env4,
					env5,
				)

				q := eventstore.Query{
					Filter: eventstore.NewFilter(
						marshalfixtures.MessageAPortableName,
					),
				}

				var g sync.WaitGroup

				fn := func() {
					defer g.Done()
					defer ginkgo.GinkgoRecover()
					events := queryEvents(tc.Context, repository, q)
					gomega.Expect(events).To(gomega.HaveLen(2))
				}

				g.Add(3)
				go fn()
				go fn()
				go fn()
				g.Wait()
			})

			ginkgo.It("does not return an error if events exist beyond the end offset", func() {
				res, err := repository.QueryEvents(tc.Context, eventstore.Query{})
				gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
				defer res.Close()

				saveEvents(
					tc.Context,
					dataStore,
					env0,
					env1,
				)

				// The implementation may or may not expose these newly
				// appended events to the caller. We simply want to ensure
				// that no error occurs.
				_, _, err = res.Next(tc.Context)
				gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
			})
		})

		ginkgo.Describe("type eventstore.Result", func() {
			ginkgo.Describe("func Next()", func() {
				ginkgo.It("returns an error if the context is canceled", func() {
					res, err := repository.QueryEvents(tc.Context, eventstore.Query{})
					gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
					defer res.Close()

					ctx, cancel := context.WithCancel(tc.Context)
					cancel()

					_, _, err = res.Next(ctx)
					gomega.Expect(err).To(gomega.Equal(context.Canceled))
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
