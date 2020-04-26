package pipeline_test

import (
	"context"

	. "github.com/dogmatiq/infix/pipeline"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

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

			seq := NewSequence(
				stage0,
				Terminate(stage1),
			)

			err := seq(context.Background(), nil, nil)
			Expect(err).ShouldNot(HaveOccurred())
		})

		It("returns the error from the first stage", func() {
			seq := NewSequence(
				Terminate(fail),
			)

			err := seq(context.Background(), nil, nil)
			Expect(err).To(MatchError("<failed>"))
		})

		It("panics if the end of the pipeline is traversed", func() {
			seq := NewSequence()

			Expect(func() {
				seq(context.Background(), nil, nil)
			}).To(Panic())
		})
	})
})
