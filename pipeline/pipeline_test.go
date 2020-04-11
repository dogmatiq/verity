package pipeline_test

import (
	"context"
	"errors"

	"github.com/dogmatiq/infix/envelope"
	. "github.com/dogmatiq/infix/pipeline"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

// pass is a Sink that always returns nil.
func pass(context.Context, *Scope, *envelope.Envelope) error {
	return nil
}

// fail is a Sink that always returns an error.
func fail(context.Context, *Scope, *envelope.Envelope) error {
	return errors.New("<error: fail() called>")
}

// noop is a stage that forwards to the next stage without doing anything.
func noop(ctx context.Context, sc *Scope, env *envelope.Envelope, next Sink) error {
	return next(ctx, sc, env)
}

var _ = Describe("type Pipeline", func() {
	Describe("func Accept()", func() {
		It("invokes the stages in order", func() {
			var order int

			stage0 := func(ctx context.Context, sc *Scope, env *envelope.Envelope, next Sink) error {
				Expect(order).To(Equal(0))
				order++
				return next(ctx, sc, env)
			}

			stage1 := func(ctx context.Context, sc *Scope, env *envelope.Envelope) error {
				Expect(order).To(Equal(1))
				order++
				return nil
			}

			p := Pipeline{
				stage0,
				Terminate(stage1),
			}

			err := p.Accept(context.Background(), &Scope{}, &envelope.Envelope{})
			Expect(err).ShouldNot(HaveOccurred())
		})

		It("returns the error from the first stage", func() {
			p := Pipeline{
				Terminate(fail),
			}

			err := p.Accept(context.Background(), &Scope{}, &envelope.Envelope{})
			Expect(err).To(MatchError("<error: fail() called>"))
		})

		It("panics if the end of the pipeline is traversed", func() {
			p := Pipeline{}

			Expect(func() {
				p.Accept(context.Background(), &Scope{}, &envelope.Envelope{})
			}).To(Panic())
		})
	})
})
