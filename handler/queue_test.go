package handler_test

import (
	"context"
	"errors"
	"time"

	. "github.com/dogmatiq/dogma/fixtures"
	. "github.com/dogmatiq/infix/fixtures"
	. "github.com/dogmatiq/infix/handler"
	"github.com/dogmatiq/infix/parcel"
	"github.com/dogmatiq/infix/persistence"
	"github.com/dogmatiq/infix/queue"
	"github.com/dogmatiq/linger/backoff"
	. "github.com/dogmatiq/marshalkit/fixtures"
	"github.com/google/go-cmp/cmp/cmpopts"
	. "github.com/jmalloc/gomegax"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"golang.org/x/sync/semaphore"
	"google.golang.org/protobuf/testing/protocmp"
)

var _ = Describe("type QueueConsumer", func() {
	var (
		ctx       context.Context
		cancel    context.CancelFunc
		dataStore *DataStoreStub
		mqueue    *queue.Queue
		pcl       parcel.Parcel
		handler   *HandlerStub
		consumer  *QueueConsumer
	)

	BeforeEach(func() {
		ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)

		dataStore = NewDataStoreStub()

		pcl = NewParcel("<id>", MessageA1)

		handler = &HandlerStub{}

		mqueue = &queue.Queue{
			Repository: dataStore,
			Marshaler:  Marshaler,
		}

		consumer = &QueueConsumer{
			Queue: mqueue,
			EntryPoint: &EntryPoint{
				Handler:   handler,
				OnSuccess: func(Result) {},
			},
			Persister:       dataStore,
			BackoffStrategy: backoff.Constant(1 * time.Millisecond), // use a small but non-zero backoff to make the tests predictable
			Semaphore:       semaphore.NewWeighted(1),
		}
	})

	JustBeforeEach(func() {
		go func() {
			defer GinkgoRecover()
			mqueue.Run(ctx)
		}()

		m := persistence.QueueMessage{
			NextAttemptAt: pcl.CreatedAt,
			Envelope:      pcl.Envelope,
		}

		_, err := dataStore.Persist(
			ctx,
			persistence.Batch{
				persistence.SaveQueueMessage{
					Message: m,
				},
			},
		)
		Expect(err).ShouldNot(HaveOccurred())

		m.Revision++

		mqueue.Add([]queue.Message{
			{
				QueueMessage: m,
				Parcel:       pcl,
			},
		})
	})

	AfterEach(func() {
		dataStore.Close()
		cancel()
	})

	Describe("func Run()", func() {
		It("passes the request to the entry-point", func() {
			handler.HandleMessageFunc = func(
				ctx context.Context,
				w UnitOfWork,
				p parcel.Parcel,
			) error {
				defer GinkgoRecover()
				defer cancel()
				Expect(p.ID()).To(Equal("<id>"))
				return nil
			}

			err := consumer.Run(ctx)
			Expect(err).To(Equal(context.Canceled))
		})

		It("returns an error if the context is canceled", func() {
			cancel()

			err := consumer.Run(ctx)
			Expect(err).To(Equal(context.Canceled))
		})

		It("returns an error if the context is canceled while waiting for the sempahore", func() {
			err := consumer.Semaphore.Acquire(ctx, 1)
			Expect(err).ShouldNot(HaveOccurred())
			defer consumer.Semaphore.Release(1)

			go func() {
				time.Sleep(100 * time.Millisecond)
				cancel()
			}()

			err = consumer.Run(ctx)
			Expect(err).To(Equal(context.Canceled))
		})

		When("the message is acknowledged", func() {
			JustBeforeEach(func() {
				runCtx, cancelRun := context.WithCancel(ctx)
				defer cancelRun()

				dataStore.PersistFunc = func(
					ctx context.Context,
					b persistence.Batch,
				) (persistence.Result, error) {
					defer cancelRun()
					return dataStore.DataStore.Persist(ctx, b)
				}

				err := consumer.Run(runCtx)
				Expect(err).To(Equal(context.Canceled))
			})

			It("removes the message from the data-store", func() {
				messages, err := dataStore.LoadQueueMessages(ctx, 1)
				Expect(err).ShouldNot(HaveOccurred())
				Expect(messages).To(BeEmpty())
			})

			It("removes the message from the queue", func() {
				ctx, cancel := context.WithTimeout(ctx, 20*time.Millisecond)
				defer cancel()

				_, err := mqueue.Pop(ctx)
				Expect(err).To(Equal(context.DeadlineExceeded))
			})
		})

		When("the message is negatively acknowledged", func() {
			JustBeforeEach(func() {
				runCtx, cancelRun := context.WithCancel(ctx)
				defer cancelRun()

				dataStore.PersistFunc = func(
					ctx context.Context,
					b persistence.Batch,
				) (persistence.Result, error) {
					defer cancelRun()
					return dataStore.DataStore.Persist(ctx, b)
				}

				handler.HandleMessageFunc = func(
					context.Context,
					UnitOfWork,
					parcel.Parcel,
				) error {
					return errors.New("<error>")
				}

				err := consumer.Run(runCtx)
				Expect(err).To(Equal(context.Canceled))
			})

			It("updates the message in the data-store", func() {
				messages, err := dataStore.LoadQueueMessages(ctx, 1)
				Expect(err).ShouldNot(HaveOccurred())
				Expect(messages).To(EqualX(
					[]persistence.QueueMessage{
						{
							Revision:      2,
							NextAttemptAt: time.Now(),
							FailureCount:  1,
							Envelope:      pcl.Envelope,
						},
					},
					protocmp.Transform(),
					cmpopts.EquateApproxTime(5*time.Millisecond),
				))
			})

			It("returns the message to the queue", func() {
				_, err := mqueue.Pop(ctx)
				Expect(err).ShouldNot(HaveOccurred())
			})

			When("there is a backoff strategy", func() {
				BeforeEach(func() {
					consumer.BackoffStrategy = backoff.Linear(5 * time.Second)
				})

				It("sets the next attempt time using the strategy", func() {
					messages, err := dataStore.LoadQueueMessages(ctx, 1)
					Expect(err).ShouldNot(HaveOccurred())
					Expect(messages).To(EqualX(
						[]persistence.QueueMessage{
							{
								Revision:      2,
								NextAttemptAt: time.Now().Add(5 * time.Second),
								FailureCount:  1,
								Envelope:      pcl.Envelope,
							},
						},
						protocmp.Transform(),
						cmpopts.EquateApproxTime(5*time.Millisecond),
					))
				})
			})
		})
	})
})
