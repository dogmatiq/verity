package queue_test

import (
	"context"
	"errors"
	"time"

	. "github.com/dogmatiq/dogma/fixtures"
	. "github.com/dogmatiq/infix/fixtures"
	"github.com/dogmatiq/infix/parcel"
	"github.com/dogmatiq/infix/persistence"
	. "github.com/dogmatiq/infix/queue"
	"github.com/dogmatiq/marshalkit/codec"
	. "github.com/dogmatiq/marshalkit/fixtures"
	. "github.com/jmalloc/gomegax"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("type Queue", func() {
	var (
		ctx                       context.Context
		cancel                    context.CancelFunc
		dataStore                 *DataStoreStub
		queue                     *Queue
		parcel0, parcel1, parcel2 parcel.Parcel
		push                      func(parcel.Parcel, ...time.Time)
	)

	BeforeEach(func() {
		ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)

		parcel0 = NewParcel("<message-0>", MessageA1)
		parcel1 = NewParcel("<message-1>", MessageA2)
		parcel2 = NewParcel("<message-2>", MessageA3)

		dataStore = NewDataStoreStub()

		queue = &Queue{
			Repository: dataStore,
			Marshaler:  Marshaler,
		}

		// push is a helper function for testing the queue that persists a
		// message then adds it to the queue.
		push = func(
			p parcel.Parcel,
			nextOptional ...time.Time,
		) {
			next := time.Now()
			for _, n := range nextOptional {
				next = n
			}

			m := persistence.QueueMessage{
				NextAttemptAt: next,
				Envelope:      p.Envelope,
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

			queue.Add([]Message{
				{
					QueueMessage: m,
					Parcel:       p,
				},
			})
		}
	})

	AfterEach(func() {
		dataStore.Close()
		cancel()
	})

	When("the queue is running", func() {
		JustBeforeEach(func() {
			go func() {
				defer GinkgoRecover()
				queue.Run(ctx)
			}()
		})

		Describe("func Pop()", func() {
			When("the queue is empty", func() {
				It("blocks until a message is pushed", func() {
					go func() {
						defer GinkgoRecover()
						time.Sleep(20 * time.Millisecond)
						push(parcel0)
					}()

					m, err := queue.Pop(ctx)
					Expect(err).ShouldNot(HaveOccurred())
					Expect(m.Parcel).To(EqualX(parcel0))
				})

				It("returns an error if the context deadline is exceeded", func() {
					ctx, cancel := context.WithTimeout(ctx, 5*time.Millisecond)
					defer cancel()

					_, err := queue.Pop(ctx)
					Expect(err).To(Equal(context.DeadlineExceeded))
				})
			})

			When("the queue is not empty", func() {
				When("the message at the front of the queue is ready for handling", func() {
					JustBeforeEach(func() {
						push(parcel0)
					})

					It("returns a message immediately", func() {
						ctx, cancel := context.WithTimeout(ctx, 5*time.Millisecond)
						defer cancel()

						m, err := queue.Pop(ctx)
						Expect(err).ShouldNot(HaveOccurred())
						Expect(m.Parcel).To(EqualX(parcel0))
					})

					It("does not return a message that has already been popped", func() {
						_, err := queue.Pop(ctx)
						Expect(err).ShouldNot(HaveOccurred())

						ctx, cancel := context.WithTimeout(ctx, 5*time.Millisecond)
						defer cancel()

						_, err = queue.Pop(ctx)
						Expect(err).To(Equal(context.DeadlineExceeded))
					})
				})

				When("the message at the front of the queue is not-ready for handling", func() {
					var next time.Time

					JustBeforeEach(func() {
						next = time.Now().Add(10 * time.Millisecond)
						push(parcel0, next)
					})

					It("blocks until the message becomes ready", func() {
						m, err := queue.Pop(ctx)
						Expect(err).ShouldNot(HaveOccurred())
						Expect(m.Parcel).To(EqualX(parcel0))
						Expect(time.Now()).To(BeTemporally(">=", next))
					})

					It("unblocks if a new message jumps the queue", func() {
						go func() {
							defer GinkgoRecover()
							time.Sleep(5 * time.Millisecond)
							push(parcel1)
						}()

						m, err := queue.Pop(ctx)
						Expect(err).ShouldNot(HaveOccurred())
						Expect(m.Parcel).To(EqualX(parcel1))
					})

					It("unblocks if a requeued message jumps the queue", func() {
						push(parcel1)

						m, err := queue.Pop(ctx)
						Expect(err).ShouldNot(HaveOccurred())
						Expect(m.Parcel).To(EqualX(parcel1))

						go func() {
							defer GinkgoRecover()
							time.Sleep(5 * time.Millisecond)
							queue.Requeue(m)
						}()

						m, err = queue.Pop(ctx)
						Expect(err).ShouldNot(HaveOccurred())
						Expect(m.Parcel).To(EqualX(parcel1))
					})

					It("returns an error if the context deadline is exceeded", func() {
						ctx, cancel := context.WithTimeout(ctx, 5*time.Millisecond)
						defer cancel()

						_, err := queue.Pop(ctx)
						Expect(err).To(Equal(context.DeadlineExceeded))
					})
				})
			})
		})

		Describe("func Add()", func() {
			It("discards messages if the buffer size limit is exceeded", func() {
				queue.BufferSize = 1

				// This push fills the buffer.
				push(parcel0)

				// This push exceeds the limit so parcel1 should not be buffered.
				push(parcel1)

				// Pop parcel0.
				_, err := queue.Pop(ctx)
				Expect(err).ShouldNot(HaveOccurred())

				// Nothing new will be loaded from the store while there is
				// anything tracked at all (this is why its important to
				// configure the buffer size larger than the number of
				// consumers).
				ctx, cancel := context.WithTimeout(ctx, 10*time.Millisecond)
				defer cancel()

				// If this doesn't timeout, it means that parcel1 was not
				// dropped from the buffer.
				_, err = queue.Pop(ctx)
				Expect(err).To(Equal(context.DeadlineExceeded))
			})

			When("the message has already been loaded from the store", func() {
				var message Message

				BeforeEach(func() {
					m := persistence.QueueMessage{
						NextAttemptAt: time.Now(),
						Envelope:      parcel0.Envelope,
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

					message = Message{
						QueueMessage: m,
						Parcel:       parcel0,
					}
				})

				It("does not duplicate the message", func() {
					// This test guarantees that a message is not duplicated if
					// it is passed to Add() after it has already been added to
					// the queue.
					//
					// This can occur when the queue is first loading if the
					// load happens to occur in between the message being
					// persisted and Add() being called.

					// We expect to get the pushed message once, by loading it
					// from the repository.
					_, err := queue.Pop(ctx)
					Expect(err).ShouldNot(HaveOccurred())

					ctx, cancel := context.WithTimeout(ctx, 10*time.Millisecond)
					defer cancel()

					// Then, even if we add it again, we do not expect it to
					// be duplicated.
					queue.Add([]Message{message})

					_, err = queue.Pop(ctx)
					Expect(err).To(Equal(context.DeadlineExceeded))
				})
			})
		})

		Describe("func Requeue()", func() {
			JustBeforeEach(func() {
				push(parcel0)
			})

			It("allows the message to be popped again", func() {
				m1, err := queue.Pop(ctx)
				Expect(err).ShouldNot(HaveOccurred())

				queue.Requeue(m1)

				m2, err := queue.Pop(ctx)
				Expect(err).ShouldNot(HaveOccurred())
				Expect(m2).To(EqualX(m1))
			})
		})

		Describe("func Remove()", func() {
			JustBeforeEach(func() {
				push(parcel0)
			})

			It("does not allow the message to be popped again", func() {
				m, err := queue.Pop(ctx)
				Expect(err).ShouldNot(HaveOccurred())

				queue.Remove(m)

				ctx, cancel := context.WithTimeout(ctx, 5*time.Millisecond)
				defer cancel()

				_, err = queue.Pop(ctx)
				Expect(err).To(Equal(context.DeadlineExceeded))
			})

			It("removes the message from the buffer", func() {
				queue.BufferSize = 1

				m, err := queue.Pop(ctx)
				Expect(err).ShouldNot(HaveOccurred())

				queue.Remove(m)

				push(parcel1, time.Now().Add(1*time.Millisecond))

				// If this pop times out, it means that parcel0 is occupying our
				// only buffer slot.
				_, err = queue.Pop(ctx)
				Expect(err).ShouldNot(HaveOccurred())
			})
		})

		Describe("func Run()", func() {
			When("messages are persisted but not in memory", func() {
				BeforeEach(func() {
					_, err := dataStore.Persist(
						ctx,
						persistence.Batch{
							persistence.SaveQueueMessage{
								Message: persistence.QueueMessage{
									NextAttemptAt: time.Now(),
									Envelope:      parcel0.Envelope,
								},
							},
							persistence.SaveQueueMessage{
								Message: persistence.QueueMessage{
									NextAttemptAt: time.Now().Add(10 * time.Millisecond),
									Envelope:      parcel1.Envelope,
								},
							},
							persistence.SaveQueueMessage{
								Message: persistence.QueueMessage{
									NextAttemptAt: time.Now().Add(5 * time.Millisecond),
									Envelope:      parcel2.Envelope,
								},
							},
						},
					)
					Expect(err).ShouldNot(HaveOccurred())
				})

				It("makes the messages available in the correct order", func() {
					m, err := queue.Pop(ctx)
					Expect(err).ShouldNot(HaveOccurred())
					Expect(m.Parcel).To(EqualX(parcel0))

					m, err = queue.Pop(ctx)
					Expect(err).ShouldNot(HaveOccurred())
					Expect(m.Parcel).To(EqualX(parcel2)) // note parcel2 is before parcel1

					m, err = queue.Pop(ctx)
					Expect(err).ShouldNot(HaveOccurred())
					Expect(m.Parcel).To(EqualX(parcel1))
				})

				When("all messages fit within the buffer", func() {
					BeforeEach(func() {
						queue.BufferSize = 100
					})

					It("does not attempt load more when the buffer is empty", func() {
						// Pop all the messages and remove them.
						for i := 0; i < 3; i++ {
							m, err := queue.Pop(ctx)
							Expect(err).ShouldNot(HaveOccurred())

							_, err = dataStore.Persist(
								ctx,
								persistence.Batch{
									persistence.RemoveQueueMessage{
										Message: m.QueueMessage,
									},
								},
							)
							Expect(err).ShouldNot(HaveOccurred())
							queue.Remove(m)
						}

						// Setup the repository to fail if it is used again.
						dataStore.LoadQueueMessagesFunc = func(
							context.Context,
							int,
						) ([]persistence.QueueMessage, error) {
							Fail("unexpected call")
							return nil, nil
						}

						ctx, cancel := context.WithTimeout(ctx, 5*time.Millisecond)
						defer cancel()

						// Call pop one last time, which would trigger a load if
						// the "exhausted" flag is not working correctly.
						_, err := queue.Pop(ctx)
						Expect(err).To(Equal(context.DeadlineExceeded))
					})
				})

				When("the messages do not fit within the buffer", func() {
					BeforeEach(func() {
						queue.BufferSize = 1
					})

					It("loads more messages once the buffer is empty", func() {
						m, err := queue.Pop(ctx)
						Expect(err).ShouldNot(HaveOccurred())

						_, err = dataStore.Persist(
							ctx,
							persistence.Batch{
								persistence.RemoveQueueMessage{
									Message: m.QueueMessage,
								},
							},
						)
						Expect(err).ShouldNot(HaveOccurred())
						queue.Remove(m)

						m, err = queue.Pop(ctx)
						Expect(err).ShouldNot(HaveOccurred())
						Expect(m.Parcel).To(EqualX(parcel2)) // note parcel2 is before parcel1
					})
				})
			})
		})
	})

	When("the queue is not running", func() {
		Describe("func Run()", func() {
			It("returns an error if messages can not be loaded from the repository", func() {
				dataStore.LoadQueueMessagesFunc = func(
					context.Context,
					int,
				) ([]persistence.QueueMessage, error) {
					return nil, errors.New("<error>")
				}

				err := queue.Run(ctx)
				Expect(err).To(MatchError("<error>"))
			})

			It("returns an error if a message can not be unmarshaled", func() {
				queue.Marshaler = &codec.Marshaler{} // an empty marshaler cannot unmarshal anything

				dataStore.LoadQueueMessagesFunc = func(
					context.Context,
					int,
				) ([]persistence.QueueMessage, error) {
					return []persistence.QueueMessage{
						{
							Envelope: parcel0.Envelope,
						},
					}, nil
				}

				err := queue.Run(ctx)
				Expect(err).To(MatchError("no codecs support the 'application/json' media-type"))
			})
		})
	})

	When("the queue has stopped", func() {
		BeforeEach(func() {
			ctx, cancel := context.WithCancel(context.Background())
			cancel() // cancel immediately

			err := queue.Run(ctx)
			Expect(err).To(Equal(context.Canceled))
		})

		Describe("func Add()", func() {
			It("does not block", func() {
				queue.Add(
					[]Message{
						{
							QueueMessage: persistence.QueueMessage{
								Revision: 1,
								Envelope: parcel0.Envelope,
							},
							Parcel: parcel0,
						},
					},
				)
			})
		})

		Describe("func Requeue()", func() {
			It("does not block", func() {
				queue.Requeue(
					Message{
						QueueMessage: persistence.QueueMessage{
							Revision: 1,
							Envelope: parcel0.Envelope,
						},
						Parcel: parcel0,
					},
				)
			})
		})

		Describe("func Remove()", func() {
			It("does not block", func() {
				queue.Remove(
					Message{
						QueueMessage: persistence.QueueMessage{
							Revision: 1,
							Envelope: parcel0.Envelope,
						},
						Parcel: parcel0,
					},
				)
			})
		})
	})
})
