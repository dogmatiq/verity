package queuestore

import (
	"context"
	"fmt"
	"time"

	dogmafixtures "github.com/dogmatiq/dogma/fixtures"
	"github.com/dogmatiq/infix/draftspecs/envelopespec"
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

			env0, env1, env2             *envelopespec.Envelope
			message0, message1, message2 *queuestore.Message
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
			env0 = infixfixtures.NewEnvelopeProto("", dogmafixtures.MessageA3)
			env1 = infixfixtures.NewEnvelopeProto("", dogmafixtures.MessageA1)
			env2 = infixfixtures.NewEnvelopeProto("", dogmafixtures.MessageA2)

			message0 = &queuestore.Message{
				NextAttemptAt: time.Now().Add(3 * time.Hour),
				Envelope:      env0,
			}

			message1 = &queuestore.Message{
				NextAttemptAt: time.Now().Add(-10 * time.Hour),
				Envelope:      env1,
			}

			message2 = &queuestore.Message{
				NextAttemptAt: time.Now().Add(2 * time.Hour),
				Envelope:      env2,
			}
		})

		ginkgo.AfterEach(func() {
			tearDown()
		})

		ginkgo.Describe("func LoadQueueMessages()", func() {
			ginkgo.It("returns an empty result if the queue is empty", func() {
				messages := loadMessages(tc.Context, repository, 10)
				gomega.Expect(messages).To(gomega.BeEmpty())
			})

			table.DescribeTable(
				"it returns messages from the queue, ordered by their next attempt time",
				func(n int, expected ...**queuestore.Message) {
					saveMessages(
						tc.Context,
						dataStore,
						message0,
						message1,
						message2,
					)

					messages := loadMessages(tc.Context, repository, n)
					gomega.Expect(messages).To(gomega.HaveLen(len(expected)))

					for i, m := range messages {
						expectMessageToEqual(
							m,
							*expected[i],
							fmt.Sprintf("message at index #%d of slice", i),
						)
					}
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

		ginkgo.It("returns an error if the context is canceled", func() {
			ctx, cancel := context.WithCancel(tc.Context)
			cancel()

			_, err := repository.LoadQueueMessages(ctx, 1)
			gomega.Expect(err).To(gomega.Equal(context.Canceled))
		})
	})
}
