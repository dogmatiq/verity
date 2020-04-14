package queuestore

import (
	"context"
	"fmt"
	"time"

	dogmafixtures "github.com/dogmatiq/dogma/fixtures"
	infixfixtures "github.com/dogmatiq/infix/fixtures"
	"github.com/dogmatiq/infix/persistence"
	"github.com/dogmatiq/infix/persistence/internal/providertest/common"
	"github.com/dogmatiq/infix/persistence/subsystem/queuestore"
	"github.com/onsi/ginkgo"
	"github.com/onsi/ginkgo/extensions/table"
	"github.com/onsi/gomega"
)

// DeclareRepositoryTests declares a functional test-suite for a specific
// queuestore.Repository implementation.
func DeclareRepositoryTests(tc *common.TestContext) {
	ginkgo.Describe("type queuestore.Repository", func() {
		var (
			dataStore  persistence.DataStore
			repository queuestore.Repository
			tearDown   func()

			item0, item1, item2 *queuestore.Item
		)

		ginkgo.BeforeEach(func() {
			dataStore, tearDown = tc.SetupDataStore()
			repository = dataStore.QueueStoreRepository()

			// Note, we use generated UUIDs for the message IDs to avoid them
			// having any predictable effect on the queue order. Likewise, we
			// don't use the fixture messages in order.
			//
			// This was noticed occurring with the SQL provider, which sorted by
			// its PRIMARY KEY, which includes the message ID.

			item0 = &queuestore.Item{
				FailureCount:  1,
				NextAttemptAt: time.Now().Add(3 * time.Hour),
				Envelope:      infixfixtures.NewEnvelope("", dogmafixtures.MessageA3),
			}

			item1 = &queuestore.Item{
				FailureCount:  2,
				NextAttemptAt: time.Now().Add(-10 * time.Hour),
				Envelope:      infixfixtures.NewEnvelope("", dogmafixtures.MessageA1),
			}

			item2 = &queuestore.Item{
				FailureCount:  3,
				NextAttemptAt: time.Now().Add(2 * time.Hour),
				Envelope:      infixfixtures.NewEnvelope("", dogmafixtures.MessageA2),
			}
		})

		ginkgo.AfterEach(func() {
			tearDown()
		})

		ginkgo.Describe("func LoadQueueMessages()", func() {
			ginkgo.It("returns an empty result if the queue is empty", func() {
				items := loadMessages(tc.Context, repository, 10)
				gomega.Expect(items).To(gomega.BeEmpty())
			})

			table.DescribeTable(
				"it returns messages from the queue, ordered by their next attempt time",
				func(n int, expected ...**queuestore.Item) {
					saveMessages(
						tc.Context,
						dataStore,
						item0,
						item1,
						item2,
					)

					items := loadMessages(tc.Context, repository, n)
					gomega.Expect(items).To(gomega.HaveLen(len(expected)))

					for i, it := range items {
						expectItemToEqual(
							it,
							*expected[i],
							fmt.Sprintf("item at index #%d of slice", i),
						)
					}
				},
				table.Entry(
					"it returns all the messages if the limit is equal the length of the queue",
					3,
					&item1, &item2, &item0,
				),
				table.Entry(
					"it returns all the messages if the limit is larger than the length of the queue",
					10,
					&item1, &item2, &item0,
				),
				table.Entry(
					"it returns the messages with the earliest next-attempt times if the limit is less than the length of the queue",
					2,
					&item1, &item2,
				),
			)
		})

		ginkgo.It("returns an error if the context is canceled", func() {
			ctx, cancel := context.WithCancel(tc.Context)
			cancel()

			_, err := repository.LoadQueueMessages(ctx, 1)
			gomega.Expect(err).To(gomega.Equal(context.Canceled))
		})
	})
}
