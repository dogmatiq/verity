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

// pass is a Sink that always returns nil.
func pass(context.Context, Request, *Response) error {
	return nil
}

// fail is a Sink that always returns an error.
func fail(context.Context, Request, *Response) error {
	return errors.New("<failed>")
}

// fail is a Sink that always panics.
func fatal(context.Context, Request, *Response) error {
	panic("<fatal>")
}

// noop is a stage that forwards to the next stage without doing anything.
func noop(ctx context.Context, req Request, res *Response, next Sink) error {
	return next(ctx, req, res)
}

var _ = Describe("func New()", func() {
	var (
		now    time.Time
		pcl    *parcel.Parcel
		tx     *TransactionStub
		stages Sequence
		req    *PipelineRequestStub
	)

	BeforeEach(func() {
		now = time.Now()
		pcl = NewParcel("<produce>", MessageP1, now, now)

		stages = nil

		req, tx = NewPipelineRequestStub(
			NewParcel("<consume>", MessageC1),
			nil,
		)
	})

	JustBeforeEach(func() {
		// Terminate the pipeline with a passing stage. This is only reached if
		// more specific BeforeEach() blocks didn't terminate the pipeline in
		// some other way.
		stages = append(stages, Terminate(pass))
	})

	When("there is no message activity", func() {
		It("does not notify the observers", func() {
			pipeline := New(
				func(
					context.Context,
					[]*parcel.Parcel,
					[]*queuestore.Item,
				) error {
					Fail("the queue observer was notified unexpectedly")
					return nil
				},
				func(
					context.Context,
					[]*parcel.Parcel,
					[]*eventstore.Item,
				) error {
					Fail("the event observer was notified unexpectedly")
					return nil
				},
				stages.Accept,
			)

			err := pipeline(context.Background(), req)
			Expect(err).ShouldNot(HaveOccurred())
		})
	})

	When("messages are enqueued", func() {
		BeforeEach(func() {
			stages = append(
				stages,
				func(ctx context.Context, req Request, res *Response, next Sink) error {
					if err := res.EnqueueMessage(ctx, tx, pcl); err != nil {
						return err
					}

					return next(ctx, req, res)
				},
			)
		})

		When("the next stage is successful", func() {
			It("notifies the queue observer", func() {
				called := true

				pipeline := New(
					func(
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
					},
					nil, // event observer
					stages.Accept,
				)

				err := pipeline(context.Background(), req)
				Expect(err).ShouldNot(HaveOccurred())
				Expect(called).To(BeTrue(), "fn was not called")
			})
		})

		When("the next stage fails", func() {
			BeforeEach(func() {
				stages = append(stages, Terminate(fail))
			})

			It("does not notify the queue observer", func() {
				pipeline := New(
					func(
						context.Context,
						[]*parcel.Parcel,
						[]*queuestore.Item,
					) error {
						Fail("the queue observer was notified unexpectedly")
						return nil
					},
					nil, // event observer
					stages.Accept,
				)

				err := pipeline(context.Background(), req)
				Expect(err).To(MatchError("<failed>"))
			})
		})

		When("the observer fails", func() {
			It("returns an error", func() {
				pipeline := New(
					func(
						context.Context,
						[]*parcel.Parcel,
						[]*queuestore.Item,
					) error {
						return errors.New("<error>")
					},
					nil, // event observer
					stages.Accept,
				)

				err := pipeline(context.Background(), req)
				Expect(err).To(MatchError("<error>"))
			})
		})
	})

	When("events are recorded", func() {
		BeforeEach(func() {
			stages = append(
				stages,
				func(ctx context.Context, req Request, res *Response, next Sink) error {
					if _, err := res.RecordEvent(ctx, tx, pcl); err != nil {
						return err
					}

					return next(ctx, req, res)
				},
			)
		})

		When("the next stage is successful", func() {
			It("notifies the queue observer", func() {
				called := true

				pipeline := New(
					nil, // queue observer
					func(
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
					},
					stages.Accept,
				)

				err := pipeline(context.Background(), req)
				Expect(err).ShouldNot(HaveOccurred())
				Expect(called).To(BeTrue(), "fn was not called")
			})
		})

		When("the next stage fails", func() {
			BeforeEach(func() {
				stages = append(stages, Terminate(fail))
			})

			It("does not notify the queue observer", func() {
				pipeline := New(
					nil, // queue observer
					func(
						context.Context,
						[]*parcel.Parcel,
						[]*eventstore.Item,
					) error {
						Fail("the queue observer was notified unexpectedly")
						return nil
					},
					stages.Accept,
				)

				err := pipeline(context.Background(), req)
				Expect(err).To(MatchError("<failed>"))
			})
		})

		When("the observer fails", func() {
			It("returns an error", func() {
				pipeline := New(
					nil, // queue observer
					func(
						context.Context,
						[]*parcel.Parcel,
						[]*eventstore.Item,
					) error {
						return errors.New("<error>")
					},
					stages.Accept,
				)

				err := pipeline(context.Background(), req)
				Expect(err).To(MatchError("<error>"))
			})
		})
	})
})
