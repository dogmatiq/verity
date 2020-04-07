package queue_test

import (
	"context"
	"time"

	. "github.com/dogmatiq/dogma/fixtures"
	"github.com/dogmatiq/infix/envelope"
	. "github.com/dogmatiq/infix/fixtures"
	"github.com/dogmatiq/infix/persistence"
	"github.com/dogmatiq/infix/persistence/provider/memory"
	"github.com/dogmatiq/infix/persistence/subsystem/queuestore"
	. "github.com/dogmatiq/infix/queue"
	. "github.com/dogmatiq/marshalkit/fixtures"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("type Consumer", func() {
	var (
		ctx        context.Context
		cancel     context.CancelFunc
		provider   *ProviderStub
		dataStore  *DataStoreStub
		repository *QueueStoreRepositoryStub
		queue      *Queue
		handler    *QueueHandlerStub
		consumer   *Consumer
		env0       *envelope.Envelope
	)

	BeforeEach(func() {
		env0 = NewEnvelope("<message-0>", MessageA1)

		ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)

		provider = &ProviderStub{
			Provider: &memory.Provider{},
		}

		ds, err := provider.Open(ctx, "<app-key>")
		Expect(err).ShouldNot(HaveOccurred())

		dataStore = ds.(*DataStoreStub)

		repository = ds.QueueStoreRepository().(*QueueStoreRepositoryStub)
		dataStore.QueueStoreRepositoryFunc = func() queuestore.Repository {
			return repository
		}

		queue = &Queue{
			DataStore: dataStore,
			Marshaler: Marshaler,
		}

		go queue.Run(ctx)

		handler = &QueueHandlerStub{}

		consumer = &Consumer{
			Queue:   queue,
			Handler: handler,
		}
	})

	AfterEach(func() {
		if dataStore != nil {
			dataStore.Close()
		}

		cancel()
	})

	Describe("func Run()", func() {
		It("returns an error if the context is canceled", func() {
			cancel()

			err := consumer.Run(ctx)
			Expect(err).To(Equal(context.Canceled))
		})

		It("passes messages to the handler", func() {
			err := queue.Push(ctx, env0)
			Expect(err).ShouldNot(HaveOccurred())

			ctx, cancel := context.WithCancel(ctx)
			defer cancel()

			handler.HandleMessageFunc = func(
				ctx context.Context,
				tx persistence.ManagedTransaction,
				env *envelope.Envelope,
			) error {
				defer cancel()
				Expect(env).To(Equal(env0))
				return nil
			}

			err = consumer.Run(ctx)
			Expect(err).To(Equal(context.Canceled))
		})
	})
})
