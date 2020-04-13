package pipeline_test

import (
	"context"
	"errors"

	. "github.com/dogmatiq/dogma/fixtures"
	"github.com/dogmatiq/infix/envelope"
	. "github.com/dogmatiq/infix/fixtures"
	"github.com/dogmatiq/infix/persistence/subsystem/eventstore"
	"github.com/dogmatiq/infix/persistence/subsystem/queuestore"
	. "github.com/dogmatiq/infix/pipeline"
	. "github.com/dogmatiq/marshalkit/fixtures"
	"github.com/golang/protobuf/proto"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Context("observer stages", func() {
	var (
		cause, effect *envelope.Envelope
		scope         *Scope
	)

	BeforeEach(func() {
		cause = NewEnvelope("<cause>", MessageC1)
		effect = NewEnvelope("<effect>", MessageE1)

		scope, _, _ = NewPipelineScope(cause, nil)
	})

	Describe("func WhenMessageEnqueued()", func() {
		It("calls the observer function if the next stage is successful", func() {
			called := true

			fn := func(
				ctx context.Context,
				pairs []queuestore.Pair,
			) error {
				called = true

				Expect(pairs).To(HaveLen(1))

				p := pairs[0]

				Expect(p.Original).To(Equal(effect))
				Expect(p.Parcel.Revision).To(BeEquivalentTo(1))
				Expect(p.Parcel.NextAttemptAt).To(BeTemporally("==", effect.CreatedAt))

				Expect(
					proto.Equal(
						p.Parcel.Envelope,
						envelope.MustMarshal(Marshaler, effect),
					),
				).To(
					BeTrue(),
					"protobuf envelope is not equal",
				)

				return nil
			}

			stage := WhenMessageEnqueued(fn)
			next := func(ctx context.Context, sc *Scope) error {
				return sc.EnqueueMessage(ctx, effect)
			}

			err := stage(context.Background(), scope, next)
			Expect(err).ShouldNot(HaveOccurred())
			Expect(called).To(BeTrue(), "fn was not called")
		})

		It("does not call the observer function if no messages were enqueued", func() {
			fn := func(
				context.Context,
				[]queuestore.Pair,
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
				[]queuestore.Pair,
			) error {
				Fail("unexpected call")
				return nil
			}

			stage := WhenMessageEnqueued(fn)
			next := func(ctx context.Context, sc *Scope) error {
				if err := sc.EnqueueMessage(ctx, effect); err != nil {
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
				[]queuestore.Pair,
			) error {
				return errors.New("<error>")
			}

			stage := WhenMessageEnqueued(fn)
			next := func(ctx context.Context, sc *Scope) error {
				return sc.EnqueueMessage(ctx, effect)
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
				pairs []eventstore.Pair,
			) error {
				called = true

				Expect(pairs).To(HaveLen(1))

				p := pairs[0]

				Expect(p.Original).To(Equal(effect))
				Expect(p.Parcel.Offset).To(BeEquivalentTo(0))

				Expect(
					proto.Equal(
						p.Parcel.Envelope,
						envelope.MustMarshal(Marshaler, effect),
					),
				).To(
					BeTrue(),
					"protobuf envelope is not equal",
				)

				return nil
			}

			stage := WhenEventRecorded(fn)
			next := func(ctx context.Context, sc *Scope) error {
				_, err := sc.RecordEvent(ctx, effect)
				return err
			}

			err := stage(context.Background(), scope, next)
			Expect(err).ShouldNot(HaveOccurred())
			Expect(called).To(BeTrue(), "fn was not called")
		})

		It("does not call the observer function if no messages were enqueued", func() {
			fn := func(
				context.Context,
				[]eventstore.Pair,
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
				[]eventstore.Pair,
			) error {
				Fail("unexpected call")
				return nil
			}

			stage := WhenEventRecorded(fn)
			next := func(ctx context.Context, sc *Scope) error {
				if _, err := sc.RecordEvent(ctx, effect); err != nil {
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
				[]eventstore.Pair,
			) error {
				return errors.New("<error>")
			}

			stage := WhenEventRecorded(fn)
			next := func(ctx context.Context, sc *Scope) error {
				_, err := sc.RecordEvent(ctx, effect)
				return err
			}

			err := stage(context.Background(), scope, next)
			Expect(err).To(MatchError("<error>"))
		})
	})
})
