package queue_test

import (
	"context"
	"errors"
	"time"

	. "github.com/dogmatiq/dogma/fixtures"
	"github.com/dogmatiq/infix/envelope"
	. "github.com/dogmatiq/infix/fixtures"
	"github.com/dogmatiq/infix/persistence/provider/memory"
	"github.com/dogmatiq/infix/persistence/subsystem/queuestore"
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
		dataStore *DataStoreStub
		queue     *Queue
		env       = NewEnvelope("<id>", MessageA1)
	)

	BeforeEach(func() {
		ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)

		provider = &ProviderStub{
			Provider: &memory.Provider{},
		}

		ds, err := provider.Open(ctx, "<app-key>")
		Expect(err).ShouldNot(HaveOccurred())

		dataStore = ds.(*DataStoreStub)

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

	Describe("func Pop()", func() {
		When("the queue is empty", func() {
			XIt("blocks until a message pushed", func() {
			})

			XIt("does not unblock if a pushed message is not ready", func() {
			})

			It("returns an error if the context deadline is exceeded", func() {
				ctx, cancel := context.WithTimeout(ctx, 5*time.Millisecond)
				defer cancel()

				sess, err := queue.Pop(ctx)
				if sess != nil {
					sess.Close()
				}
				Expect(err).To(Equal(context.DeadlineExceeded))
			})
		})

		When("the queue is not empty", func() {
			When("the message at the front of the queue is ready for handling", func() {

			})

			When("the message at the front of the queue is not-ready for handling", func() {
				BeforeEach(func() {
					// It's not possible to push a message with a future
					// next-attempt time via Queue's interface, so we need to
					// persist something directly, then tell the queue that it's
					// buffer is stale.
					err := persistence.WithTransaction(
						ctx,
						dataStore,
						func(tx persistence.ManagedTransaction) error {
							return tx.SaveMessageToQueue(
								ctx,
								&queuestore.Message{
									NextAttemptAt: time.Now().Add(10 * time.Millisecond),
									Envelope:      envelope.MustMarshal(Marshaler, env),
								},
							)
						},
					)
					Expect(err).ShouldNot(HaveOccurred())
				})

				XIt("blocks until the message is ready", func() {
				})

				XIt("unblocks if a new message jumps the queue", func() {
				})

				It("returns an error if the context deadline is exceeded", func() {
					ctx, cancel := context.WithTimeout(ctx, 5*time.Millisecond)
					defer cancel()

					sess, err := queue.Pop(ctx)
					if sess != nil {
						sess.Close()
					}
					Expect(err).To(Equal(context.DeadlineExceeded))
				})
			})
		})

		It("returns a non-nil session", func() {
			err := queue.Push(ctx, env)
			Expect(err).ShouldNot(HaveOccurred())

			sess, err := queue.Pop(ctx)
			Expect(err).ShouldNot(HaveOccurred())

			err = sess.Close()
			Expect(err).ShouldNot(HaveOccurred())
		})

		It("provides the unmarshaled message envelope", func() {
			err := queue.Push(ctx, env)
			Expect(err).ShouldNot(HaveOccurred())

			sess, err := queue.Pop(ctx)
			Expect(err).ShouldNot(HaveOccurred())
			defer sess.Close()

			Expect(sess.Envelope()).To(Equal(env))
		})

		It("returns an error if the context is canceled", func() {
			cancel()

			sess, err := queue.Pop(ctx)
			if sess != nil {
				sess.Close()
			}
			Expect(err).To(Equal(context.Canceled))
		})

		It("starts a transaction", func() {
			err := queue.Push(ctx, env)
			Expect(err).ShouldNot(HaveOccurred())

			sess, err := queue.Pop(ctx)
			Expect(err).ShouldNot(HaveOccurred())
			defer sess.Close()

			tx := sess.Tx()
			Expect(tx).NotTo(BeNil())
		})

		It("leaves the message on the queue if the transaction can not be started", func() {
			err := queue.Push(ctx, env)
			Expect(err).ShouldNot(HaveOccurred())

			dataStore.BeginFunc = func(
				ctx context.Context,
			) (persistence.Transaction, error) {
				dataStore.BeginFunc = nil
				return nil, errors.New("<error>")
			}

			sess, err := queue.Pop(ctx)
			if sess != nil {
				sess.Close()
			}
			Expect(err).To(MatchError("<error>"))

			sess, err = queue.Pop(ctx)
			Expect(err).ShouldNot(HaveOccurred())
			defer sess.Close()
		})
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
