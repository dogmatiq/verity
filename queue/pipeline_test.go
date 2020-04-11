package queue_test

import (
	"context"
	"errors"
	"time"

	. "github.com/dogmatiq/dogma/fixtures"
	"github.com/dogmatiq/infix/envelope"
	. "github.com/dogmatiq/infix/fixtures"
	"github.com/dogmatiq/infix/handler"
	"github.com/dogmatiq/infix/persistence"
	"github.com/dogmatiq/infix/persistence/provider/memory"
	"github.com/dogmatiq/infix/persistence/subsystem/queuestore"
	. "github.com/dogmatiq/infix/queue"
	"github.com/dogmatiq/linger/backoff"
	. "github.com/dogmatiq/marshalkit/fixtures"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("type PipelinePump", func() {
	var (
		ctx        context.Context
		cancel     context.CancelFunc
		provider   *ProviderStub
		dataStore  *DataStoreStub
		repository *QueueStoreRepositoryStub
		queue      *Queue
		pump       *PipelinePump
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

		pump = &PipelinePump{
			Queue: queue,
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

			err := pump.Run(ctx)
			Expect(err).To(Equal(context.Canceled))
		})

		It("pushes messages down the pipeline", func() {
			push(ctx, queue, env0)

			pump.Pipeline = func(
				ctx context.Context,
				tx persistence.ManagedTransaction,
				env *envelope.Envelope,
			) error {
				defer GinkgoRecover()
				defer cancel()
				Expect(env).To(Equal(env0))
				return nil
			}

			err := pump.Run(ctx)
			Expect(err).To(Equal(context.Canceled))
		})

		It("acks the message if the pipeline succeeds", func() {
			// Configure the pump as single-threaded with no backoff.
			pump.Semaphore = handler.NewSemaphore(1)
			pump.BackoffStrategy = backoff.Constant(0)

			// Push a message.
			push(ctx, queue, env0)

			// Push another message after a delay guaranteeing that they don't
			// have the same next-attempt time.
			time.Sleep(5 * time.Millisecond)
			push(ctx, queue, env1)

			// Then setup a pipeline that expects to see env0 then env1. If the
			// env0 is not ack'd we will see it twice before we see env1.
			count := 0
			pump.Pipeline = func(
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

			err := pump.Run(ctx)
			Expect(err).To(Equal(context.Canceled))
		})

		It("nacks the message if the pipeline fails", func() {
			// Configure the pump with a short backoff.
			delay := 5 * time.Millisecond
			pump.BackoffStrategy = backoff.Constant(delay)

			push(ctx, queue, env0)

			// Then setup a pipeline that expects to see env0 twice. If the env0
			// is not nack'd it will not be retried.
			var first time.Time
			pump.Pipeline = func(
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

			err := pump.Run(ctx)
			Expect(err).To(Equal(context.Canceled))
		})

		It("returns if the context is canceled while waiting for the sempahore", func() {
			pump.Semaphore = handler.NewSemaphore(1)

			err := pump.Semaphore.Acquire(ctx)
			Expect(err).ShouldNot(HaveOccurred())
			defer pump.Semaphore.Release()

			go func() {
				time.Sleep(100 * time.Millisecond)
				cancel()
			}()

			push(ctx, queue, env0)

			err = pump.Run(ctx)
			Expect(err).To(Equal(context.Canceled))
		})
	})
})
