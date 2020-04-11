package queue_test

import (
	"context"
	"errors"
	"time"

	"github.com/dogmatiq/dodeca/logging"

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
		logger     *logging.BufferedLogger
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

		logger = &logging.BufferedLogger{}

		pump = &PipelinePump{
			Queue:           queue,
			Semaphore:       handler.NewSemaphore(1),
			BackoffStrategy: backoff.Constant(5 * time.Millisecond),
			Logger:          logger,
		}
	})

	JustBeforeEach(func() {
		go queue.Run(ctx)
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

		When("there is a valid message on the queue", func() {
			BeforeEach(func() {
				push(ctx, queue, env0)
			})

			It("pushes messages down the pipeline", func() {
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
				// Push another message after a delay guaranteeing that it doesn't
				// have the same next-attempt time as env0.
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

			It("uses the backoff strategy to compute the delay time", func() {
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
					Expect(time.Now()).To(
						BeTemporally(">=", first.Add(5*time.Millisecond)),
					)

					cancel()

					return nil
				}

				err := pump.Run(ctx)
				Expect(err).To(Equal(context.Canceled))
			})

			It("logs when the message is ack'd", func() {
				pump.Pipeline = func(
					context.Context,
					persistence.ManagedTransaction,
					*envelope.Envelope,
				) error {
					cancel()
					return nil
				}

				err := pump.Run(ctx)
				Expect(err).To(Equal(context.Canceled))
				Expect(logger.Messages()).To(ContainElement(
					logging.BufferedLogMessage{
						Message: "= <message-0>  ∵ <cause>  ⋲ <correlation>  ▼    fixtures.MessageA ● {A1}",
					},
				))
			})

			It("logs when the message is nack'd", func() {
				pump.Pipeline = func(
					context.Context,
					persistence.ManagedTransaction,
					*envelope.Envelope,
				) error {
					cancel()
					return errors.New("<error>")
				}

				err := pump.Run(ctx)
				Expect(err).To(Equal(context.Canceled))
				Expect(logger.Messages()).To(ContainElement(
					logging.BufferedLogMessage{
						Message: "= <message-0>  ∵ <cause>  ⋲ <correlation>  ▽ ✖  fixtures.MessageA ● <error> ● next retry in 5ms ● {A1}",
					},
				))
			})

			It("nacks the message if the transaction can not be started", func() {
				dataStore.BeginFunc = func(ctx context.Context) (persistence.Transaction, error) {
					cancel()
					dataStore.BeginFunc = nil
					return nil, errors.New("<error>")
				}

				err := pump.Run(ctx)
				Expect(err).To(Equal(context.Canceled))
				Expect(logger.Messages()).To(ContainElement(
					logging.BufferedLogMessage{
						Message: "= <message-0>  ∵ <cause>  ⋲ <correlation>  ▽ ✖  fixtures.MessageA ● <error> ● next retry in 5ms ● {A1}",
					},
				))
			})

			It("returns an error if the context is canceled while waiting for the sempahore", func() {
				err := pump.Semaphore.Acquire(ctx)
				Expect(err).ShouldNot(HaveOccurred())
				defer pump.Semaphore.Release()

				go func() {
					time.Sleep(100 * time.Millisecond)
					cancel()
				}()

				err = pump.Run(ctx)
				Expect(err).To(Equal(context.Canceled))
			})
		})

		When("there is an invalid message on the queue", func() {
			BeforeEach(func() {
				// Persist env0 with malformed data.
				err := persistence.WithTransaction(
					ctx,
					dataStore,
					func(tx persistence.ManagedTransaction) error {
						env := envelope.MustMarshal(Marshaler, env0)
						env.MetaData.CorrelationId = ""

						m := &queuestore.Message{
							NextAttemptAt: time.Now(),
							Envelope:      env,
						}

						return tx.SaveMessageToQueue(ctx, m)
					},
				)
				Expect(err).ShouldNot(HaveOccurred())
			})

			It("nacks the message", func() {
				pump.Pipeline = func(
					context.Context,
					persistence.ManagedTransaction,
					*envelope.Envelope,
				) error {
					defer GinkgoRecover()
					Fail("message unexpected sent via the pipeline")
					return nil
				}

				ctx, cancel := context.WithTimeout(ctx, 5*time.Millisecond)
				defer cancel()

				err := pump.Run(ctx)
				Expect(err).To(Equal(context.DeadlineExceeded))

				Expect(logger.Messages()).To(ContainElement(
					logging.BufferedLogMessage{
						Message: "= <message-0>  ∵ -  ⋲ -  ▽ ✖  correlation ID must not be empty ● next retry in 5ms",
					},
				))
			})
		})
	})
})
