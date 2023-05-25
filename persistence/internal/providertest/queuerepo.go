package providertest

import (
	"context"
	"time"

	dogmafixtures "github.com/dogmatiq/dogma/fixtures"
	verityfixtures "github.com/dogmatiq/verity/fixtures"
	"github.com/dogmatiq/verity/persistence"
	"github.com/jmalloc/gomegax"
	"github.com/onsi/ginkgo/extensions/table"
	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
)

// declareQueueRepositoryTests declares a functional test-suite for a specific
// persistence.QueueRepository implementation.
func declareQueueRepositoryTests(tc *TestContext) {
	ginkgo.Describe("type persistence.QueueRepository", func() {
		var (
			dataStore persistence.DataStore
			tearDown  func()

			now                          time.Time
			message0, message1, message2 persistence.QueueMessage
		)

		ginkgo.BeforeEach(func() {
			dataStore, tearDown = tc.SetupDataStore()

			// Note, we use generated UUIDs for the message IDs to avoid them
			// having any predictable effect on the queue order. Likewise, we
			// don't use the fixture messages in order.
			//
			// This was noticed occurring with the SQL provider, which sorted by
			// its PRIMARY KEY, which includes the message ID.

			now = time.Now().Truncate(time.Millisecond) // we only expect NextAttemptAt to have millisecond precision

			message0 = persistence.QueueMessage{
				FailureCount:  1,
				NextAttemptAt: now.Add(3 * time.Hour),
				Envelope:      verityfixtures.NewEnvelope("", dogmafixtures.MessageA3),
			}

			message1 = persistence.QueueMessage{
				FailureCount:  2,
				NextAttemptAt: now.Add(-10 * time.Hour),
				Envelope:      verityfixtures.NewEnvelope("", dogmafixtures.MessageA1),
			}

			message2 = persistence.QueueMessage{
				FailureCount:  3,
				NextAttemptAt: now.Add(2 * time.Hour),
				Envelope:      verityfixtures.NewEnvelope("", dogmafixtures.MessageA2),
			}
		})

		ginkgo.AfterEach(func() {
			tearDown()
		})

		ginkgo.Describe("func LoadQueueMessages()", func() {
			ginkgo.It("returns an empty result if the queue is empty", func() {
				messages := loadQueueMessages(tc.Context, dataStore, 10)
				gomega.Expect(messages).To(gomega.BeEmpty())
			})

			table.DescribeTable(
				"it returns messages from the queue, ordered by their next attempt time",
				func(n int, pointers ...*persistence.QueueMessage) {
					persist(
						tc.Context,
						dataStore,
						persistence.SaveQueueMessage{
							Message: message0,
						},
						persistence.SaveQueueMessage{
							Message: message1,
						},
						persistence.SaveQueueMessage{
							Message: message2,
						},
					)

					message0.Revision++
					message1.Revision++
					message2.Revision++

					var expected []persistence.QueueMessage
					for _, p := range pointers {
						expected = append(expected, *p)
					}

					messages := loadQueueMessages(tc.Context, dataStore, n)
					gomega.Expect(messages).To(gomegax.EqualX(expected))
				},
				table.Entry(
					"it returns all the messages if the limit is equal the length of the queue",
					3,
					&message1, &message2, &message0,
				),
				table.Entry(
					"it returns all the messages if the limit is larger than the length of the queue",
					10,
					&message1, &message2, &message0,
				),
				table.Entry(
					"it returns the messages with the earliest next-attempt times if the limit is less than the length of the queue",
					2,
					&message1, &message2,
				),
			)
		})

		ginkgo.It("does not block if the context is canceled", func() {
			// This test ensures that the implementation returns
			// immediately, either with a context.Canceled error, or with
			// the correct result.

			persist(
				tc.Context,
				dataStore,
				persistence.SaveQueueMessage{
					Message: message0,
				},
			)

			ctx, cancel := context.WithCancel(tc.Context)
			cancel()

			messages, err := dataStore.LoadQueueMessages(ctx, 1)
			if err != nil {
				gomega.Expect(err).To(gomega.Equal(context.Canceled))
			} else {
				gomega.Expect(messages).To(gomega.HaveLen(1))
			}
		})
	})
}
