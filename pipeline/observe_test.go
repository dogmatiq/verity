package pipeline_test

import (
	"context"
	"errors"

	. "github.com/dogmatiq/dogma/fixtures"
	"github.com/dogmatiq/infix/envelope"
	. "github.com/dogmatiq/infix/fixtures"
	"github.com/dogmatiq/infix/persistence"
	. "github.com/dogmatiq/infix/pipeline"
	. "github.com/dogmatiq/marshalkit/fixtures"
	"github.com/golang/protobuf/proto"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Context("observer stages", func() {
	var (
		tx      *TransactionStub
		session *SessionStub
		scope   *Scope
		env     *envelope.Envelope
	)

	BeforeEach(func() {
		env = NewEnvelope("<id>", MessageA1)

		tx = &TransactionStub{}

		session = &SessionStub{
			TxFunc: func(ctx context.Context) (persistence.ManagedTransaction, error) {
				return tx, nil
			},
		}

		scope = &Scope{
			Session:   session,
			Marshaler: Marshaler,
		}
	})

	Describe("func WhenMessageEnqueued()", func() {
		It("calls the observer function if the next stage is successful", func() {
			called := true

			fn := func(
				ctx context.Context,
				messages []EnqueuedMessage,
			) error {
				called = true

				Expect(messages).To(HaveLen(1))

				m := messages[0]

				Expect(m.Memory).To(Equal(env))
				Expect(m.Persisted.Revision).To(BeEquivalentTo(1))
				Expect(m.Persisted.NextAttemptAt).To(BeTemporally("==", env.CreatedAt))

				Expect(
					proto.Equal(
						m.Persisted.Envelope,
						envelope.MustMarshal(Marshaler, env),
					),
				).To(
					BeTrue(),
					"protobuf envelope is not equal",
				)

				return nil
			}

			stage := WhenMessageEnqueued(fn)
			next := func(ctx context.Context, sc *Scope) error {
				return sc.EnqueueMessage(ctx, env)
			}

			err := stage(context.Background(), scope, next)
			Expect(err).ShouldNot(HaveOccurred())
			Expect(called).To(BeTrue(), "fn was not called")
		})

		It("does not call the observer function if no messages were enqueued", func() {
			fn := func(
				context.Context,
				[]EnqueuedMessage,
			) error {
				Fail("unexpected call")
				return nil
			}

			stage := WhenMessageEnqueued(fn)

			err := stage(context.Background(), scope, pass)
			Expect(err).ShouldNot(HaveOccurred())
		})

		It("does not call the observer function if the next stage fails", func() {
			fn := func(
				context.Context,
				[]EnqueuedMessage,
			) error {
				Fail("unexpected call")
				return nil
			}

			stage := WhenMessageEnqueued(fn)
			next := func(ctx context.Context, sc *Scope) error {
				if err := sc.EnqueueMessage(ctx, env); err != nil {
					return err
				}

				return errors.New("<error>")
			}

			err := stage(context.Background(), scope, next)
			Expect(err).To(MatchError("<error>"))
		})

		It("returns an error if an observer function fails", func() {
			fn := func(
				context.Context,
				[]EnqueuedMessage,
			) error {
				return errors.New("<error>")
			}

			stage := WhenMessageEnqueued(fn)
			next := func(ctx context.Context, sc *Scope) error {
				return sc.EnqueueMessage(ctx, env)
			}

			err := stage(context.Background(), scope, next)
			Expect(err).To(MatchError("<error>"))
		})
	})

	Describe("func WhenEventRecorded()", func() {
		It("calls the observer function if the next stage is successful", func() {
			called := true

			fn := func(
				ctx context.Context,
				messages []RecordedEvent,
			) error {
				called = true

				Expect(messages).To(HaveLen(1))

				m := messages[0]

				Expect(m.Memory).To(Equal(env))
				Expect(m.Persisted.Offset).To(BeEquivalentTo(0))

				Expect(
					proto.Equal(
						m.Persisted.Envelope,
						envelope.MustMarshal(Marshaler, env),
					),
				).To(
					BeTrue(),
					"protobuf envelope is not equal",
				)

				return nil
			}

			stage := WhenEventRecorded(fn)
			next := func(ctx context.Context, sc *Scope) error {
				_, err := sc.RecordEvent(ctx, env)
				return err
			}

			err := stage(context.Background(), scope, next)
			Expect(err).ShouldNot(HaveOccurred())
			Expect(called).To(BeTrue(), "fn was not called")
		})

		It("does not call the observer function if no messages were enqueued", func() {
			fn := func(
				context.Context,
				[]RecordedEvent,
			) error {
				Fail("unexpected call")
				return nil
			}

			stage := WhenEventRecorded(fn)

			err := stage(context.Background(), scope, pass)
			Expect(err).ShouldNot(HaveOccurred())
		})

		It("does not call the observer function if the next stage fails", func() {
			fn := func(
				context.Context,
				[]RecordedEvent,
			) error {
				Fail("unexpected call")
				return nil
			}

			stage := WhenEventRecorded(fn)
			next := func(ctx context.Context, sc *Scope) error {
				if _, err := sc.RecordEvent(ctx, env); err != nil {
					return err
				}

				return errors.New("<error>")
			}

			err := stage(context.Background(), scope, next)
			Expect(err).To(MatchError("<error>"))
		})

		It("returns an error if an observer function fails", func() {
			fn := func(
				context.Context,
				[]RecordedEvent,
			) error {
				return errors.New("<error>")
			}

			stage := WhenEventRecorded(fn)
			next := func(ctx context.Context, sc *Scope) error {
				_, err := sc.RecordEvent(ctx, env)
				return err
			}

			err := stage(context.Background(), scope, next)
			Expect(err).To(MatchError("<error>"))
		})
	})
})
