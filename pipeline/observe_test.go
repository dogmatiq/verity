package pipeline_test

import (
	"context"
	"errors"

	. "github.com/dogmatiq/dogma/fixtures"
	"github.com/dogmatiq/infix/envelope"
	. "github.com/dogmatiq/infix/fixtures"
	. "github.com/dogmatiq/infix/pipeline"
	. "github.com/dogmatiq/marshalkit/fixtures"
	"github.com/golang/protobuf/proto"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Context("observer stages", func() {
	var (
		tx            *TransactionStub
		scope         *Scope
		cause, effect *envelope.Envelope
	)

	BeforeEach(func() {
		tx = &TransactionStub{}

		scope = &Scope{
			Tx:        tx,
			Marshaler: Marshaler,
		}

		cause = NewEnvelope("<cause>", MessageC1)
		effect = NewEnvelope("<effect>", MessageE1)
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

				Expect(m.Memory).To(Equal(effect))
				Expect(m.Persisted.Revision).To(BeEquivalentTo(1))
				Expect(m.Persisted.NextAttemptAt).To(BeTemporally("==", effect.CreatedAt))

				Expect(
					proto.Equal(
						m.Persisted.Envelope,
						envelope.MustMarshal(Marshaler, effect),
					),
				).To(
					BeTrue(),
					"protobuf envelope is not equal",
				)

				return nil
			}

			stage := WhenMessageEnqueued(fn)
			next := func(ctx context.Context, sc *Scope, _ *envelope.Envelope) error {
				return sc.EnqueueMessage(ctx, effect)
			}

			err := stage(context.Background(), scope, cause, next)
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

			err := stage(context.Background(), scope, cause, pass)
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
			next := func(ctx context.Context, sc *Scope, _ *envelope.Envelope) error {
				if err := sc.EnqueueMessage(ctx, effect); err != nil {
					return err
				}

				return errors.New("<error>")
			}

			err := stage(context.Background(), scope, cause, next)
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
			next := func(ctx context.Context, sc *Scope, _ *envelope.Envelope) error {
				return sc.EnqueueMessage(ctx, effect)
			}

			err := stage(context.Background(), scope, cause, next)
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

				Expect(m.Memory).To(Equal(effect))
				Expect(m.Persisted.Offset).To(BeEquivalentTo(0))

				Expect(
					proto.Equal(
						m.Persisted.Envelope,
						envelope.MustMarshal(Marshaler, effect),
					),
				).To(
					BeTrue(),
					"protobuf envelope is not equal",
				)

				return nil
			}

			stage := WhenEventRecorded(fn)
			next := func(ctx context.Context, sc *Scope, _ *envelope.Envelope) error {
				_, err := sc.RecordEvent(ctx, effect)
				return err
			}

			err := stage(context.Background(), scope, cause, next)
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

			err := stage(context.Background(), scope, cause, pass)
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
			next := func(ctx context.Context, sc *Scope, _ *envelope.Envelope) error {
				if _, err := sc.RecordEvent(ctx, effect); err != nil {
					return err
				}

				return errors.New("<error>")
			}

			err := stage(context.Background(), scope, cause, next)
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
			next := func(ctx context.Context, sc *Scope, _ *envelope.Envelope) error {
				_, err := sc.RecordEvent(ctx, effect)
				return err
			}

			err := stage(context.Background(), scope, cause, next)
			Expect(err).To(MatchError("<error>"))
		})
	})
})
