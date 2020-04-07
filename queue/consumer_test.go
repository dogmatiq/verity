package queue_test

import (
	"context"
	"errors"
	"time"

	. "github.com/dogmatiq/dogma/fixtures"
	"github.com/dogmatiq/infix/envelope"
	. "github.com/dogmatiq/infix/fixtures"
	"github.com/dogmatiq/infix/persistence"
	"github.com/dogmatiq/infix/persistence/provider/memory"
	"github.com/dogmatiq/infix/persistence/subsystem/queuestore"
	. "github.com/dogmatiq/infix/queue"
	"github.com/dogmatiq/infix/semaphore"
	"github.com/dogmatiq/linger/backoff"
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
		env0, env1 *envelope.Envelope
	)

	BeforeEach(func() {
		env0 = NewEnvelope("<message-0>", MessageA1)
		env1 = NewEnvelope("<message-1>", MessageA2)

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

			handler.HandleMessageFunc = func(
				ctx context.Context,
				tx persistence.ManagedTransaction,
				env *envelope.Envelope,
			) error {
				defer GinkgoRecover()
				defer cancel()
				Expect(env).To(Equal(env0))
				return nil
			}

			err = consumer.Run(ctx)
			Expect(err).To(Equal(context.Canceled))
		})

		It("commits the session if the handler succeeds", func() {
			// Configure the consumer as single-threaded with no backoff.
			consumer.Semaphore = semaphore.New(1)
			consumer.BackoffStrategy = backoff.Constant(0)

			// Push a message.
			err := queue.Push(ctx, env0)
			Expect(err).ShouldNot(HaveOccurred())

			// Push another message after a delay guaranteeing that they don't
			// have the same next-attempt time.
			time.Sleep(5 * time.Millisecond)
			err = queue.Push(ctx, env1)
			Expect(err).ShouldNot(HaveOccurred())

			// Then setup a handler that expects to see env0 then env1. If the
			// session for env0 is not committed we will see it twice before we
			// see env1.
			count := 0
			handler.HandleMessageFunc = func(
				ctx context.Context,
				tx persistence.ManagedTransaction,
				env *envelope.Envelope,
			) error {
				defer GinkgoRecover()

				switch count {
				case 0:
					Expect(env).To(Equal(env0))
				case 1:
					Expect(env).To(Equal(env1))
					cancel()
				default:
					Fail("too many calls")
				}

				count++

				return nil
			}

			err = consumer.Run(ctx)
			Expect(err).To(Equal(context.Canceled))
		})

		It("rolls the session back if the handler fails", func() {
			// Configure the consumer with a short backoff.
			delay := 5 * time.Millisecond
			consumer.BackoffStrategy = backoff.Constant(delay)

			err := queue.Push(ctx, env0)
			Expect(err).ShouldNot(HaveOccurred())

			// Then setup a handler that expects to see env0 twice. If the
			// session for env0 is not rolled back we it will not be retried.
			var first time.Time
			handler.HandleMessageFunc = func(
				ctx context.Context,
				tx persistence.ManagedTransaction,
				env *envelope.Envelope,
			) error {
				defer GinkgoRecover()

				if first.IsZero() {
					first = time.Now()
					return errors.New("<error>")
				}

				// The message should be retried after the delay configured by
				// the backoff.
				Expect(time.Now()).To(BeTemporally(">=", first.Add(delay)))
				cancel()

				return nil
			}

			err = consumer.Run(ctx)
			Expect(err).To(Equal(context.Canceled))

			handler.HandleMessageFunc = func(
				ctx context.Context,
				tx persistence.ManagedTransaction,
				env *envelope.Envelope,
			) error {
				return errors.New("<error>")
			}
		})

		It("returns if the context is canceled while waiting for the sempahore", func() {
			consumer.Semaphore = semaphore.New(1)

			err := consumer.Semaphore.Acquire(ctx)
			Expect(err).ShouldNot(HaveOccurred())
			defer consumer.Semaphore.Release()

			go func() {
				time.Sleep(100 * time.Millisecond)
				cancel()
			}()

			err = queue.Push(ctx, env0)
			Expect(err).ShouldNot(HaveOccurred())

			err = consumer.Run(ctx)
			Expect(err).To(Equal(context.Canceled))
		})
	})
})
