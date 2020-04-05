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
			It("blocks until a message is pushed", func() {
				go func() {
					defer GinkgoRecover()
					time.Sleep(20 * time.Millisecond)
					err := queue.Push(ctx, env)
					Expect(err).ShouldNot(HaveOccurred())
				}()

				sess, err := queue.Pop(ctx)
				Expect(err).ShouldNot(HaveOccurred())
				defer sess.Close()
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
			BeforeEach(func() {
				err := queue.Push(ctx, env)
				Expect(err).ShouldNot(HaveOccurred())
			})

			When("the message at the front of the queue is ready for handling", func() {
				It("returns immediately", func() {
					ctx, cancel := context.WithTimeout(ctx, 5*time.Millisecond)
					defer cancel()

					sess, err := queue.Pop(ctx)
					Expect(err).ShouldNot(HaveOccurred())
					defer sess.Close()
				})

				It("leaves the message on the queue if the transaction can not be started", func() {
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

			When("the message at the front of the queue is not-ready for handling", func() {
				var next time.Time

				BeforeEach(func() {
					next = time.Now().Add(10 * time.Millisecond)

					sess, err := queue.Pop(ctx)
					Expect(err).ShouldNot(HaveOccurred())
					defer sess.Close()

					err = sess.Rollback(ctx, next)
					Expect(err).ShouldNot(HaveOccurred())
				})

				It("blocks until the message becomes ready", func() {
					sess, err := queue.Pop(ctx)
					Expect(err).ShouldNot(HaveOccurred())
					defer sess.Close()

					Expect(time.Now()).To(BeTemporally(">=", next))
				})

				It("unblocks if a new message jumps the queue", func() {
					new := NewEnvelope("<new>", MessageX1)

					go func() {
						defer GinkgoRecover()
						time.Sleep(5 * time.Millisecond)
						err := queue.Push(ctx, new)
						Expect(err).ShouldNot(HaveOccurred())
					}()

					sess, err := queue.Pop(ctx)
					Expect(err).ShouldNot(HaveOccurred())
					defer sess.Close()

					Expect(sess.Envelope()).To(Equal(new))
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
			dataStore.BeginFunc = func(
				ctx context.Context,
			) (persistence.Transaction, error) {
				return nil, errors.New("<error>")
			}

			err := queue.Push(ctx, env)
			Expect(err).Should(HaveOccurred())
		})

		XIt("discards an element if the buffer is full", func() {

		})
	})
})
