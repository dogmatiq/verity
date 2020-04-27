package queue_test

import (
	"context"
	"errors"
	"time"

	. "github.com/dogmatiq/dogma/fixtures"
	. "github.com/dogmatiq/infix/fixtures"
	"github.com/dogmatiq/infix/parcel"
	"github.com/dogmatiq/infix/persistence"
	"github.com/dogmatiq/infix/persistence/subsystem/queuestore"
	. "github.com/dogmatiq/infix/queue"
	. "github.com/dogmatiq/marshalkit/fixtures"
	. "github.com/jmalloc/gomegax"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

// push is a helper function for testing the queue that persists a message to
// the queue then begins tracking it.
func push(
	ctx context.Context,
	q *Queue,
	p *parcel.Parcel,
	nextOptional ...time.Time,
) {
	next := time.Now()
	for _, n := range nextOptional {
		next = n
	}

	i := &queuestore.Item{
		NextAttemptAt: next,
		Envelope:      p.Envelope,
	}

	err := persistence.WithTransaction(
		ctx,
		q.DataStore,
		func(tx persistence.ManagedTransaction) error {
			return tx.SaveMessageToQueue(ctx, i)
		},
	)
	Expect(err).ShouldNot(HaveOccurred())

	i.Revision++

	err = q.Track(ctx, p, i)
	Expect(err).ShouldNot(HaveOccurred())
}

var _ = Describe("type Queue", func() {
	var (
		ctx                       context.Context
		cancel                    context.CancelFunc
		dataStore                 *DataStoreStub
		repository                *QueueStoreRepositoryStub
		queue                     *Queue
		parcel0, parcel1, parcel2 *parcel.Parcel
	)

	BeforeEach(func() {
		ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)

		parcel0 = NewParcel("<message-0>", MessageA1)
		parcel1 = NewParcel("<message-1>", MessageA2)
		parcel2 = NewParcel("<message-2>", MessageA3)

		dataStore = NewDataStoreStub()
		repository = dataStore.QueueStoreRepository().(*QueueStoreRepositoryStub)
		dataStore.QueueStoreRepositoryFunc = func() queuestore.Repository {
			return repository
		}

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

	When("the queue is running", func() {
		JustBeforeEach(func() {
			go queue.Run(ctx) // nolint
		})

		Describe("func Pop()", func() {
			When("the queue is empty", func() {
				It("blocks until a message is pushed", func() {
					go func() {
						defer GinkgoRecover()
						time.Sleep(20 * time.Millisecond)
						push(ctx, queue, parcel0)
					}()

					req, err := queue.Pop(ctx)
					Expect(err).ShouldNot(HaveOccurred())
					defer req.Close()
				})

				It("returns an error if the context deadline is exceeded", func() {
					ctx, cancel := context.WithTimeout(ctx, 5*time.Millisecond)
					defer cancel()

					req, err := queue.Pop(ctx)
					if req != nil {
						req.Close()
					}
					Expect(err).To(Equal(context.DeadlineExceeded))
				})
			})

			When("the queue is not empty", func() {
				When("the message at the front of the queue is ready for handling", func() {
					BeforeEach(func() {
						push(ctx, queue, parcel0)
					})

					It("returns a request immediately", func() {
						ctx, cancel := context.WithTimeout(ctx, 5*time.Millisecond)
						defer cancel()

						req, err := queue.Pop(ctx)
						Expect(err).ShouldNot(HaveOccurred())
						defer req.Close()
					})
				})

				When("the message at the front of the queue is not-ready for handling", func() {
					var next time.Time

					BeforeEach(func() {
						next = time.Now().Add(10 * time.Millisecond)
						push(ctx, queue, parcel0, next)
					})

					It("blocks until the message becomes ready", func() {
						req, err := queue.Pop(ctx)
						Expect(err).ShouldNot(HaveOccurred())
						defer req.Close()

						Expect(time.Now()).To(BeTemporally(">=", next))
					})

					It("unblocks if a new message jumps the queue", func() {
						go func() {
							defer GinkgoRecover()
							time.Sleep(5 * time.Millisecond)
							push(ctx, queue, parcel1)
						}()

						req, err := queue.Pop(ctx)
						Expect(err).ShouldNot(HaveOccurred())
						defer req.Close()

						Expect(req.Envelope()).To(EqualX(parcel1.Envelope))
					})

					It("returns an error if the context deadline is exceeded", func() {
						ctx, cancel := context.WithTimeout(ctx, 5*time.Millisecond)
						defer cancel()

						req, err := queue.Pop(ctx)
						if req != nil {
							req.Close()
						}
						Expect(err).To(Equal(context.DeadlineExceeded))
					})
				})
			})

			When("messages are persisted but not in memory", func() {
				BeforeEach(func() {
					items := []*queuestore.Item{
						{
							NextAttemptAt: time.Now(),
							Envelope:      parcel0.Envelope,
						},
						{
							NextAttemptAt: time.Now().Add(10 * time.Millisecond),
							Envelope:      parcel1.Envelope,
						},
						{
							NextAttemptAt: time.Now().Add(5 * time.Millisecond),
							Envelope:      parcel2.Envelope,
						},
					}

					err := persistence.WithTransaction(
						ctx,
						dataStore,
						func(tx persistence.ManagedTransaction) error {
							for _, i := range items {
								if err := tx.SaveMessageToQueue(ctx, i); err != nil {
									return err
								}
							}
							return nil
						},
					)
					Expect(err).ShouldNot(HaveOccurred())
				})

				It("returns a request for a message loaded from the store", func() {
					req, err := queue.Pop(ctx)
					Expect(err).ShouldNot(HaveOccurred())
					defer req.Close()

					Expect(req.Envelope()).To(EqualX(parcel0.Envelope))
				})
			})
		})

		Describe("func Track()", func() {
			It("panics if the message has not been persisted", func() {
				Expect(func() {
					err := queue.Track(
						ctx,
						parcel0,
						&queuestore.Item{
							Revision: 0, // 0 == not persisted
						},
					)
					Expect(err).ShouldNot(HaveOccurred())
				}).To(Panic())
			})

			It("discards an element if the buffer is full", func() {
				queue.BufferSize = 1

				// This push fills the buffer.
				push(ctx, queue, parcel0)

				// This push exceeds the limit so env1 should not be buffered.
				push(ctx, queue, parcel1)

				// Acquire a request for parcel0, but don't commit it.
				req, err := queue.Pop(ctx)
				Expect(err).ShouldNot(HaveOccurred())
				defer req.Close()

				// Nothing new will be loaded from the store while there is
				// anything tracked at all (this is why its important to
				// configure the buffer size larger than the number of
				// consumers).
				ctx, cancel := context.WithTimeout(ctx, 10*time.Millisecond)
				defer cancel()

				req, err = queue.Pop(ctx)
				if req != nil {
					req.Close()
				}
				Expect(err).To(Equal(context.DeadlineExceeded))
			})

			When("a message is tracked while loading from the store", func() {
				BeforeEach(func() {
					repository.LoadQueueMessagesFunc = func(
						ctx context.Context,
						n int,
					) ([]*queuestore.Item, error) {
						push(ctx, queue, parcel0)
						return repository.Repository.LoadQueueMessages(ctx, n)
					}
				})

				It("does not duplicate the message", func() {
					// We expect to get the pushed message once.
					req, err := queue.Pop(ctx)
					Expect(err).ShouldNot(HaveOccurred())
					defer req.Close()

					ctx, cancel := context.WithTimeout(ctx, 10*time.Millisecond)
					defer cancel()

					// But not twice.
					req, err = queue.Pop(ctx)
					if req != nil {
						req.Close()
					}
					Expect(err).To(Equal(context.DeadlineExceeded))
				})
			})
		})
	})

	When("the queue is not running", func() {
		Describe("func Track()", func() {
			It("returns an error if the deadline is exceeded", func() {
				i := &queuestore.Item{
					Revision: 1,
					Envelope: parcel0.Envelope,
				}

				// It's an implementation detail, but the internal channel used
				// to start tracking is buffered at the same size as the overall
				// buffer size limit.
				//
				// We can't set it to zero, because that will fallback to the
				// default. We also can't start the queue, otherwise it'll start
				// reading from this channel and nothing will block.
				//
				// Instead, we set it to one, and "fill" the channel with a
				// request to ensure that it will block.
				queue.BufferSize = 1
				err := queue.Track(ctx, parcel0, i)
				Expect(err).ShouldNot(HaveOccurred())

				// Setup a short deadline for the test.
				ctx, cancel := context.WithTimeout(ctx, 5*time.Millisecond)
				defer cancel()

				err = queue.Track(ctx, parcel0, i)
				Expect(err).To(Equal(context.DeadlineExceeded))
			})
		})

		Describe("fun Run()", func() {
			It("returns an error if messages can not be loaded from the repository", func() {
				repository.LoadQueueMessagesFunc = func(
					context.Context,
					int,
				) ([]*queuestore.Item, error) {
					return nil, errors.New("<error>")
				}

				err := queue.Run(ctx)
				Expect(err).To(MatchError("<error>"))
			})
		})
	})

	When("the queue has stopped", func() {
		BeforeEach(func() {
			ctx, cancel := context.WithCancel(context.Background())
			cancel() // cancel immediately

			// It's an implementation detail, but the internal channel used to
			// start tracking is buffered at the same size as the overall buffer
			// size limit.
			//
			// We can't set it to zero, because that will fallback to the
			// default. We also can't start the queue, otherwise it'll start
			// reading from this channel and nothing will block.
			//
			// Instead, we set it to one, and "fill" the channel with a request
			// to ensure that it will block.
			queue.BufferSize = 1

			err := queue.Run(ctx)
			Expect(err).To(Equal(context.Canceled))
		})

		Describe("func Track()", func() {
			It("does not block", func() {
				err := queue.Track(
					ctx,
					parcel0,
					&queuestore.Item{
						Revision: 1,
						Envelope: parcel0.Envelope,
					},
				)
				Expect(err).ShouldNot(HaveOccurred())
			})

			It("does not block, even if the internal buffer is full", func() {
				// Fill the buffer.
				err := queue.Track(
					ctx,
					parcel0,
					&queuestore.Item{
						Revision: 1,
						Envelope: parcel0.Envelope,
					},
				)
				Expect(err).ShouldNot(HaveOccurred())

				// Ensure it doesn't block once full.
				err = queue.Track(
					ctx,
					parcel1,
					&queuestore.Item{
						Revision: 1,
						Envelope: parcel0.Envelope,
					},
				)
				Expect(err).ShouldNot(HaveOccurred())
			})
		})
	})
})
