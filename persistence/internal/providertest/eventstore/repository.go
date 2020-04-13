package eventstore

import (
	"context"
	"fmt"
	"sync"

	dogmafixtures "github.com/dogmatiq/dogma/fixtures"
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

			parcel0, parcel1, parcel2, parcel3, parcel4, parcel5 *eventstore.Parcel
		)

		ginkgo.BeforeEach(func() {
			dataStore, tearDown = tc.SetupDataStore()
			repository = dataStore.EventStoreRepository()

			parcel0 = &eventstore.Parcel{
				Offset:   0,
				Envelope: infixfixtures.NewEnvelopeProto("<message-0>", dogmafixtures.MessageA1),
			}

			parcel1 = &eventstore.Parcel{
				Offset:   1,
				Envelope: infixfixtures.NewEnvelopeProto("<message-1>", dogmafixtures.MessageB1),
			}

			parcel2 = &eventstore.Parcel{
				Offset:   2,
				Envelope: infixfixtures.NewEnvelopeProto("<message-2>", dogmafixtures.MessageC1),
			}

			parcel3 = &eventstore.Parcel{
				Offset:   3,
				Envelope: infixfixtures.NewEnvelopeProto("<message-3>", dogmafixtures.MessageA2),
			}

			parcel4 = &eventstore.Parcel{
				Offset:   4,
				Envelope: infixfixtures.NewEnvelopeProto("<message-4>", dogmafixtures.MessageB2),
			}

			parcel5 = &eventstore.Parcel{
				Offset:   5,
				Envelope: infixfixtures.NewEnvelopeProto("<message-5>", dogmafixtures.MessageC2),
			}

			// Setup some different source handler values to test the aggregate
			// instance filtering.
			parcel0.Envelope.MetaData.Source.Handler.Key = "<aggregate>"
			parcel0.Envelope.MetaData.Source.InstanceId = "<instance-a>"

			parcel1.Envelope.MetaData.Source.Handler.Key = "<aggregate>"
			parcel1.Envelope.MetaData.Source.InstanceId = "<instance-b>"

			parcel2.Envelope.MetaData.Source.Handler.Key = "<aggregate>"
			parcel2.Envelope.MetaData.Source.InstanceId = "<instance-a>"

			parcel3.Envelope.MetaData.Source.Handler.Key = "<aggregate>"
			parcel3.Envelope.MetaData.Source.InstanceId = "<instance-b>"
		})

		ginkgo.AfterEach(func() {
			tearDown()
		})

		ginkgo.Describe("func QueryEvents()", func() {
			ginkgo.It("returns an empty result if the store is empty", func() {
				parcels := queryEvents(tc.Context, repository, eventstore.Query{})
				gomega.Expect(parcels).To(gomega.BeEmpty())
			})

			table.DescribeTable(
				"it returns a result containing the events that match the query criteria",
				func(q eventstore.Query, expected ...**eventstore.Parcel) {
					saveEvents(
						tc.Context,
						dataStore,
						parcel0.Envelope,
						parcel1.Envelope,
						parcel2.Envelope,
						parcel3.Envelope,
						parcel4.Envelope,
						parcel5.Envelope,
					)

					parcels := queryEvents(tc.Context, repository, q)
					gomega.Expect(parcels).To(gomega.HaveLen(len(expected)))

					for i, p := range parcels {
						expectParcelToEqual(
							p,
							*expected[i],
							fmt.Sprintf("parcel at index #%d of slice", i),
						)
					}
				},
				table.Entry(
					"it includes all events by default",
					eventstore.Query{},
					&parcel0, &parcel1, &parcel2, &parcel3, &parcel4, &parcel5,
				),
				table.Entry(
					"it honours the minimum offset",
					eventstore.Query{MinOffset: 3},
					&parcel3, &parcel4, &parcel5,
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
					&parcel0, &parcel2, &parcel3, &parcel5,
				),
				table.Entry(
					"it honours the aggregate instance filter",
					eventstore.Query{
						AggregateHandlerKey: "<aggregate>",
						AggregateInstanceID: "<instance-a>",
					},
					&parcel0, &parcel2,
				),
			)

			ginkgo.It("allows concurrent filtered consumers for the same application", func() {
				saveEvents(
					tc.Context,
					dataStore,
					parcel0.Envelope,
					parcel1.Envelope,
					parcel2.Envelope,
					parcel3.Envelope,
					parcel4.Envelope,
					parcel5.Envelope,
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
					parcels := queryEvents(tc.Context, repository, q)
					gomega.Expect(parcels).To(gomega.HaveLen(2))
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
					parcel0.Envelope,
					parcel1.Envelope,
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
