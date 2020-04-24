package pipeline_test

import (
	"context"
	"errors"
	"time"

	. "github.com/dogmatiq/dogma/fixtures"
	. "github.com/dogmatiq/infix/fixtures"
	"github.com/dogmatiq/infix/parcel"
	"github.com/dogmatiq/infix/persistence/subsystem/eventstore"
	"github.com/dogmatiq/infix/persistence/subsystem/queuestore"
	. "github.com/dogmatiq/infix/pipeline"
	. "github.com/jmalloc/gomegax"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Context("observer stages", func() {
	var (
		now time.Time
		pcl *parcel.Parcel
		tx  *TransactionStub
		req *PipelineRequestStub
		res *Response
	)

	BeforeEach(func() {
		now = time.Now()
		pcl = NewParcel("<produce>", MessageP1, now, now)

		req, tx = NewPipelineRequestStub(
			NewParcel("<consume>", MessageC1),
			nil,
		)

		res = &Response{}
	})

	Describe("func WhenMessageEnqueued()", func() {
		It("calls the observer function if the next stage is successful", func() {
			called := true

			fn := func(
				ctx context.Context,
				parcels []*parcel.Parcel,
				items []*queuestore.Item,
			) error {
				called = true

				Expect(parcels).To(EqualX(
					[]*parcel.Parcel{pcl},
				))

				Expect(items).To(EqualX(
					[]*queuestore.Item{
						{
							Revision:      1,
							NextAttemptAt: now,
							Envelope:      pcl.Envelope,
						},
					},
				))

				return nil
			}

			stage := WhenMessageEnqueued(fn)
			next := func(ctx context.Context, req Request, res *Response) error {
				return res.EnqueueMessage(ctx, tx, pcl)
			}

			err := stage(context.Background(), req, res, next)
			Expect(err).ShouldNot(HaveOccurred())
			Expect(called).To(BeTrue(), "fn was not called")
		})

		It("does not call the observer function if no messages were enqueued", func() {
			fn := func(
				context.Context,
				[]*parcel.Parcel,
				[]*queuestore.Item,
			) error {
				Fail("unexpected call")
				return nil
			}

			stage := WhenMessageEnqueued(fn)

			err := stage(context.Background(), req, res, pass)
			Expect(err).ShouldNot(HaveOccurred())
		})

		It("does not call the observer function if the next stage fails", func() {
			fn := func(
				context.Context,
				[]*parcel.Parcel,
				[]*queuestore.Item,
			) error {
				Fail("unexpected call")
				return nil
			}

			stage := WhenMessageEnqueued(fn)
			next := func(ctx context.Context, req Request, res *Response) error {
				if err := res.EnqueueMessage(ctx, tx, pcl); err != nil {
					return err
				}

				return errors.New("<error>")
			}

			err := stage(context.Background(), req, res, next)
			Expect(err).To(MatchError("<error>"))
		})

		It("returns an error if an observer function fails", func() {
			fn := func(
				context.Context,
				[]*parcel.Parcel,
				[]*queuestore.Item,
			) error {
				return errors.New("<error>")
			}

			stage := WhenMessageEnqueued(fn)
			next := func(ctx context.Context, req Request, res *Response) error {
				return res.EnqueueMessage(ctx, tx, pcl)
			}

			err := stage(context.Background(), req, res, next)
			Expect(err).To(MatchError("<error>"))
		})
	})

	Describe("func WhenEventRecorded()", func() {
		It("calls the observer function if the next stage is successful", func() {
			called := true

			fn := func(
				ctx context.Context,
				parcels []*parcel.Parcel,
				items []*eventstore.Item,
			) error {
				called = true

				Expect(parcels).To(EqualX(
					[]*parcel.Parcel{pcl},
				))

				Expect(items).To(EqualX(
					[]*eventstore.Item{
						{
							Offset:   0,
							Envelope: pcl.Envelope,
						},
					},
				))

				return nil
			}

			stage := WhenEventRecorded(fn)
			next := func(ctx context.Context, req Request, res *Response) error {
				_, err := res.RecordEvent(ctx, tx, pcl)
				return err
			}

			err := stage(context.Background(), req, res, next)
			Expect(err).ShouldNot(HaveOccurred())
			Expect(called).To(BeTrue(), "fn was not called")
		})

		It("does not call the observer function if no messages were enqueued", func() {
			fn := func(
				context.Context,
				[]*parcel.Parcel,
				[]*eventstore.Item,
			) error {
				Fail("unexpected call")
				return nil
			}

			stage := WhenEventRecorded(fn)

			err := stage(context.Background(), req, res, pass)
			Expect(err).ShouldNot(HaveOccurred())
		})

		It("does not call the observer function if the next stage fails", func() {
			fn := func(
				context.Context,
				[]*parcel.Parcel,
				[]*eventstore.Item,
			) error {
				Fail("unexpected call")
				return nil
			}

			stage := WhenEventRecorded(fn)
			next := func(ctx context.Context, req Request, res *Response) error {
				if _, err := res.RecordEvent(ctx, tx, pcl); err != nil {
					return err
				}

				return errors.New("<error>")
			}

			err := stage(context.Background(), req, res, next)
			Expect(err).To(MatchError("<error>"))
		})

		It("returns an error if an observer function fails", func() {
			fn := func(
				context.Context,
				[]*parcel.Parcel,
				[]*eventstore.Item,
			) error {
				return errors.New("<error>")
			}

			stage := WhenEventRecorded(fn)
			next := func(ctx context.Context, req Request, res *Response) error {
				_, err := res.RecordEvent(ctx, tx, pcl)
				return err
			}

			err := stage(context.Background(), req, res, next)
			Expect(err).To(MatchError("<error>"))
		})
	})
})
