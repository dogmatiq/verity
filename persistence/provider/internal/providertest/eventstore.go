package providertest

import (
	"context"

	"github.com/dogmatiq/configkit/message"
	dogmafixtures "github.com/dogmatiq/dogma/fixtures"
	infixfixtures "github.com/dogmatiq/infix/fixtures"
	"github.com/dogmatiq/infix/persistence"
	"github.com/dogmatiq/infix/persistence/eventstore"
	"github.com/onsi/ginkgo"
	"github.com/onsi/ginkgo/extensions/table"
	"github.com/onsi/gomega"
)

func declareEventStoreTests(
	ctx *context.Context,
	in *In,
	out *Out,
) {
	ginkgo.Context("package eventstore", func() {
		var (
			dataStore  persistence.DataStore
			repository eventstore.Repository

			env0 = infixfixtures.NewEnvelope("<message-0>", dogmafixtures.MessageA1)
			env1 = infixfixtures.NewEnvelope("<message-1>", dogmafixtures.MessageB1)

			event0 = &eventstore.Event{
				Offset:   0,
				MetaData: env0.MetaData,
				Packet:   env0.Packet,
			}

			event1 = &eventstore.Event{
				Offset:   1,
				MetaData: env1.MetaData,
				Packet:   env1.Packet,
			}
		)

		ginkgo.BeforeEach(func() {
			var err error
			dataStore, err = out.Provider.Open(*ctx, "<app-key>")
			gomega.Expect(err).ShouldNot(gomega.HaveOccurred())

			repository = dataStore.EventStoreRepository()
		})

		ginkgo.AfterEach(func() {
			if dataStore != nil {
				dataStore.Close()
			}
		})

		ginkgo.Describe("type Repository (interface)", func() {
			ginkgo.Describe("func Query()", func() {
				ginkgo.It("returns an empty result if the store is empty", func() {
					res, err := repository.QueryEvents(*ctx, eventstore.Query{})
					gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
					defer res.Close()

					gomega.Expect(res.Next()).To(gomega.BeFalse())
				})

				table.DescribeTable(
					"it includes all of the events that match the query criteria",
					func(q eventstore.Query, expected ...*eventstore.Event) {
						ginkgo.By("starting a transaction")

						tx, err := dataStore.Begin(*ctx)
						gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
						defer tx.Rollback()

						ginkgo.By("saving some events")

						err = tx.SaveEvents(*ctx, env0, env1)
						gomega.Expect(err).ShouldNot(gomega.HaveOccurred())

						ginkgo.By("committing the transaction")

						err = tx.Commit(*ctx)
						gomega.Expect(err).ShouldNot(gomega.HaveOccurred())

						ginkgo.By("querying the events")

						res, err := repository.QueryEvents(*ctx, eventstore.Query{})
						gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
						defer res.Close()

						ginkgo.By("iterating through the result")

						count := len(expected)
						index := 0

						for res.Next() {
							gomega.Expect(index).To(
								gomega.BeNumerically("<", count),
								"too many events included in the result",
							)

							ev, err := res.Get(*ctx)
							gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
							gomega.Expect(ev).To(gomega.Equal(expected[index]))

							index++
						}

						gomega.Expect(index).To(
							gomega.Equal(count),
							"too few events included in the result",
						)
					},
					table.Entry(
						"the default query includes all events",
						eventstore.Query{},
						event0, event1,
					),
				)

				ginkgo.It("does not include any events that are saved after the query is performed", func() {
					ginkgo.By("querying the events")

					res, err := repository.QueryEvents(*ctx, eventstore.Query{})
					gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
					defer res.Close()

					ginkgo.By("starting a transaction")

					tx, err := dataStore.Begin(*ctx)
					gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
					defer tx.Rollback()

					ginkgo.By("saving some events")

					err = tx.SaveEvents(*ctx, env0, env1)
					gomega.Expect(err).ShouldNot(gomega.HaveOccurred())

					ginkgo.By("committing the transaction")

					err = tx.Commit(*ctx)
					gomega.Expect(err).ShouldNot(gomega.HaveOccurred())

					ginkgo.By("iterating through the result")

					gomega.Expect(res.Next()).To(gomega.BeFalse())
				})

				ginkgo.It("panics if the query types are non-nil, but empty", func() {
					q := eventstore.Query{
						Types: message.NewTypeSet(),
					}

					gomega.Expect(func() {
						repository.QueryEvents(*ctx, q)
					}).To(gomega.Panic())
				})
			})
		})

		ginkgo.Describe("type ResultSet (interface)", func() {
			ginkgo.Describe("func Close()", func() {
				ginkgo.It("does not return an error if the result set is open", func() {
					res, err := repository.QueryEvents(*ctx, eventstore.Query{})
					gomega.Expect(err).ShouldNot(gomega.HaveOccurred())

					err = res.Close()
					gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
				})

				ginkgo.It("does not panic if the result set is already closed", func() {
					// The return value of res.Close() for a result set that is
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
