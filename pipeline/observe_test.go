package pipeline_test

import (
	"context"
	"errors"
	"time"

	. "github.com/dogmatiq/dogma/fixtures"
	. "github.com/dogmatiq/infix/fixtures"
	. "github.com/dogmatiq/infix/internal/x/gomegax"
	"github.com/dogmatiq/infix/parcel"
	"github.com/dogmatiq/infix/persistence/subsystem/eventstore"
	"github.com/dogmatiq/infix/persistence/subsystem/queuestore"
	. "github.com/dogmatiq/infix/pipeline"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Context("observer stages", func() {
	var (
		now   time.Time
		pcl   *parcel.Parcel
		scope *Scope
	)

	BeforeEach(func() {
		now = time.Now()
		pcl = NewParcel("<produce>", MessageP1, now, now)

		scope, _, _ = NewPipelineScope(
			NewEnvelopeProto("<consume>", MessageC1),
			nil,
		)
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
				Expect(pairs[0]).To(EqualX(
					queuestore.Pair{
						Parcel: &queuestore.Parcel{
							Revision:      1,
							NextAttemptAt: now,
							Envelope:      pcl.Envelope,
						},
						Message: pcl.Message,
					},
				))

				return nil
			}

			stage := WhenMessageEnqueued(fn)
			next := func(ctx context.Context, sc *Scope) error {
				return sc.EnqueueMessage(ctx, pcl)
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
				if err := sc.EnqueueMessage(ctx, pcl); err != nil {
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
				return sc.EnqueueMessage(ctx, pcl)
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
				Expect(pairs[0]).To(EqualX(
					eventstore.Pair{
						Parcel: &eventstore.Parcel{
							Offset:   0,
							Envelope: pcl.Envelope,
						},
						Message: pcl.Message,
					},
				))

				return nil
			}

			stage := WhenEventRecorded(fn)
			next := func(ctx context.Context, sc *Scope) error {
				_, err := sc.RecordEvent(ctx, pcl)
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
				if _, err := sc.RecordEvent(ctx, pcl); err != nil {
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
				_, err := sc.RecordEvent(ctx, pcl)
				return err
			}

			err := stage(context.Background(), scope, next)
			Expect(err).To(MatchError("<error>"))
		})
	})
})
