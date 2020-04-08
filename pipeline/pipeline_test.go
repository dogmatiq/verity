package pipeline_test

import (
	"context"
	"errors"

	. "github.com/dogmatiq/infix/pipeline"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

// pass is a Sink that always returns nil.
func pass(context.Context, *Scope) error {
	return nil
}

// fail is a Sink that always returns an error.
func fail(context.Context, *Scope) error {
	return errors.New("<error: fail() called>")
}

// noop is stage that forwards to the next stage without doing anything.
func noop(ctx context.Context, sc *Scope, next Sink) error {
	return next(ctx, sc)
}

var _ = Describe("func New()", func() {
	It("invokes the stages in order", func() {
		var order int

		stage0 := func(ctx context.Context, sc *Scope, next Sink) error {
			Expect(order).To(Equal(0))
			order++
			return next(ctx, sc)
		}

		stage1 := func(ctx context.Context, sc *Scope) error {
			Expect(order).To(Equal(1))
			order++
			return nil
		}

		p := New(stage0, Terminate(stage1))

		err := p(context.Background(), &Scope{})
		Expect(err).ShouldNot(HaveOccurred())
	})

	It("returns the error from the first stage", func() {
		p := New(Terminate(fail))

		err := p(context.Background(), &Scope{})
		Expect(err).To(MatchError("<error: fail() called>"))
	})

	It("panics if the end of the pipeline is traversed", func() {
		p := New(noop)

		Expect(func() {
			p(context.Background(), &Scope{})
		}).To(Panic())
	})

	It("panics if no stages are given", func() {
		Expect(func() {
			New()
		}).To(Panic())
	})
})
