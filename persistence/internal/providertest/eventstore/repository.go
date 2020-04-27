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

			item0, item1, item2, item3, item4, item5 *eventstore.Item
		)

		ginkgo.BeforeEach(func() {
			dataStore, tearDown = tc.SetupDataStore()
			repository = dataStore.EventStoreRepository()

			item0 = &eventstore.Item{
				Offset:   0,
				Envelope: infixfixtures.NewEnvelope("<message-0>", dogmafixtures.MessageA1),
			}

			item1 = &eventstore.Item{
				Offset:   1,
				Envelope: infixfixtures.NewEnvelope("<message-1>", dogmafixtures.MessageB1),
			}

			item2 = &eventstore.Item{
				Offset:   2,
				Envelope: infixfixtures.NewEnvelope("<message-2>", dogmafixtures.MessageC1),
			}

			item3 = &eventstore.Item{
				Offset:   3,
				Envelope: infixfixtures.NewEnvelope("<message-3>", dogmafixtures.MessageA2),
			}

			item4 = &eventstore.Item{
				Offset:   4,
				Envelope: infixfixtures.NewEnvelope("<message-4>", dogmafixtures.MessageB2),
			}

			item5 = &eventstore.Item{
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
					saveEvents(tc.Context, dataStore, env)
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

			ginkgo.It("returns an error if the context is canceled", func() {
				ctx, cancel := context.WithCancel(tc.Context)
				cancel()

				_, err := repository.NextEventOffset(ctx)
				gomega.Expect(err).To(gomega.Equal(context.Canceled))
			})
		})

		ginkgo.Describe("func QueryEvents()", func() {
			ginkgo.It("returns an empty result if the store is empty", func() {
				items := queryEvents(tc.Context, repository, eventstore.Query{})
				gomega.Expect(items).To(gomega.BeEmpty())
			})

			table.DescribeTable(
				"it returns a result containing the events that match the query criteria",
				func(q eventstore.Query, expected ...**eventstore.Item) {
					saveEvents(
						tc.Context,
						dataStore,
						item0.Envelope,
						item1.Envelope,
						item2.Envelope,
						item3.Envelope,
						item4.Envelope,
						item5.Envelope,
					)

					items := queryEvents(tc.Context, repository, q)
					gomega.Expect(items).To(gomega.HaveLen(len(expected)))

					for i, item := range items {
						expectItemToEqual(
							item,
							*expected[i],
							fmt.Sprintf("item at index #%d of slice", i),
						)
					}
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

			ginkgo.It("allows concurrent filtered consumers for the same application", func() {
				saveEvents(
					tc.Context,
					dataStore,
					item0.Envelope,
					item1.Envelope,
					item2.Envelope,
					item3.Envelope,
					item4.Envelope,
					item5.Envelope,
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
					items := queryEvents(tc.Context, repository, q)
					gomega.Expect(items).To(gomega.HaveLen(2))
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
				defer res.Close() // nolint

				saveEvents(
					tc.Context,
					dataStore,
					item0.Envelope,
					item1.Envelope,
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
					defer res.Close() // nolint

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
