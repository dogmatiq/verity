package queue_test

import (
	"context"
	"time"

	. "github.com/dogmatiq/dogma/fixtures"
	"github.com/dogmatiq/infix/envelope"
	. "github.com/dogmatiq/infix/fixtures"
	"github.com/dogmatiq/infix/persistence"
	"github.com/dogmatiq/infix/persistence/provider/memory"
	. "github.com/dogmatiq/infix/queue"
	. "github.com/dogmatiq/marshalkit/fixtures"
	"github.com/golang/protobuf/proto"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("type Queue", func() {
	var (
		ctx       context.Context
		cancel    context.CancelFunc
		provider  *ProviderStub
		dataStore persistence.DataStore
		queue     *Queue
		env       = NewEnvelope("<id>", MessageA1)
	)

	BeforeEach(func() {
		ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)

		provider = &ProviderStub{
			Provider: &memory.Provider{},
		}

		var err error
		dataStore, err = provider.Open(ctx, "<app-key>")
		Expect(err).ShouldNot(HaveOccurred())

		queue = &Queue{
			DataStore: dataStore,
			Marshaler: Marshaler,
		}
	})

	AfterEach(func() {
		if dataStore != nil {
			dataStore.Close()
		}

		cancel()
	})

	Describe("func Push()", func() {
		It("persists the message in the queue store", func() {
			err := queue.Push(ctx, env)
			Expect(err).ShouldNot(HaveOccurred())

			messages, err := dataStore.QueueStoreRepository().LoadQueueMessages(ctx, 2)
			Expect(err).ShouldNot(HaveOccurred())
			Expect(messages).To(HaveLen(1))

			m := messages[0]
			penv := envelope.MustMarshal(Marshaler, env)
			Expect(proto.Equal(m.Envelope, penv)).To(BeTrue())
		})

		It("schedules the message for immediate handling", func() {
			err := queue.Push(ctx, env)
			Expect(err).ShouldNot(HaveOccurred())

			messages, err := dataStore.QueueStoreRepository().LoadQueueMessages(ctx, 2)
			Expect(err).ShouldNot(HaveOccurred())
			Expect(messages).To(HaveLen(1))

			m := messages[0]
			Expect(m.NextAttemptAt).To(BeTemporally("~", time.Now()))
		})

		It("returns an error if the transaction can not be begun", func() {
			dataStore.Close()

			err := queue.Push(ctx, env)
			Expect(err).Should(HaveOccurred())
		})
	})
})
