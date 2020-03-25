package providertest

import (
	"context"

	"github.com/dogmatiq/infix/persistence"
	"github.com/dogmatiq/infix/persistence/eventstore"
	"github.com/onsi/ginkgo"
	"github.com/onsi/gomega"
)

func declareEventStoreTests(
	ctx *context.Context,
	in *In,
	out *Out,
) {
	ginkgo.Describe("package eventstore (interfaces)", func() {
		var (
			dataStore  persistence.DataStore
			repository eventstore.Repository
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

		ginkgo.Describe("func Query()", func() {
			ginkgo.It("returns an empty result set the store is empty", func() {
				res, err := repository.QueryEvents(*ctx, eventstore.Query{})
				gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
				defer res.Close()

				gomega.Expect(res.Next()).To(gomega.BeFalse())
			})

			ginkgo.It("allows the result set to be closed", func() {
				// This test ensures that res.Close() can be called at least
				// once, but nothing further, as the behavior of Close() on
				// subsequent calls is implementation-defined.
				res, err := repository.QueryEvents(*ctx, eventstore.Query{})
				gomega.Expect(err).ShouldNot(gomega.HaveOccurred())

				err = res.Close()
				gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
			})
		})
	})
}
