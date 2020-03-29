package providertest

import (
	"context"

	"github.com/dogmatiq/infix/persistence"
	"github.com/dogmatiq/infix/persistence/subsystem/queue"
	"github.com/onsi/ginkgo"
	"github.com/onsi/gomega"
)

func declareQueueTests(
	ctx *context.Context,
	in *In,
	out *Out,
) {
	ginkgo.Context("package queue", func() {
		var (
			provider      persistence.Provider
			closeProvider func()
			dataStore     persistence.DataStore
			repository    queue.Repository
		)

		ginkgo.BeforeEach(func() {
			provider, closeProvider = out.NewProvider()

			var err error
			dataStore, err = provider.Open(*ctx, "<app-key>")
			gomega.Expect(err).ShouldNot(gomega.HaveOccurred())

			repository = dataStore.QueueRepository()
		})

		ginkgo.AfterEach(func() {
			if dataStore != nil {
				dataStore.Close()
			}

			if closeProvider != nil {
				closeProvider()
			}
		})

		ginkgo.Describe("type Repository (interface)", func() {
			ginkgo.Describe("func LoadQueuedMessages()", func() {
				ginkgo.It("returns an empty result if the queue is empty", func() {
					messages, err := repository.LoadQueuedMessages(*ctx, 10)
					gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
					gomega.Expect(messages).To(gomega.BeEmpty())
				})
			})
		})
	})
}
