package pipeline_test

import (
	"context"
	"errors"

	. "github.com/dogmatiq/infix/pipeline"
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

var _ = Describe("type Pipeline", func() {
	Describe("func Accept()", func() {
		It("invokes the stages in order", func() {
			var order int

			stage0 := func(ctx context.Context, req Request, res *Response, next Sink) error {
				Expect(order).To(Equal(0))
				order++
				return next(ctx, req, res)
			}

			stage1 := func(ctx context.Context, req Request, res *Response) error {
				Expect(order).To(Equal(1))
				order++
				return nil
			}

			p := Pipeline{
				stage0,
				Terminate(stage1),
			}

			err := p.Accept(context.Background(), nil)
			Expect(err).ShouldNot(HaveOccurred())
		})

		It("returns the error from the first stage", func() {
			p := Pipeline{
				Terminate(fail),
			}

			err := p.Accept(context.Background(), nil)
			Expect(err).To(MatchError("<failed>"))
		})

		It("panics if the end of the pipeline is traversed", func() {
			p := Pipeline{}

			Expect(func() {
				p.Accept(context.Background(), nil)
			}).To(Panic())
		})
	})
})
