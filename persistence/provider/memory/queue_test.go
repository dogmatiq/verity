package memory_test

import (
	"context"

	. "github.com/dogmatiq/dogma/fixtures"
	"github.com/dogmatiq/infix/envelope"
	. "github.com/dogmatiq/infix/fixtures"
	"github.com/dogmatiq/infix/internal/testing/queuetest"
	. "github.com/dogmatiq/infix/persistence/provider/memory"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("type Queue (standard test suite)", func() {
	var queue *Queue

	queuetest.Declare(
		func(ctx context.Context, in queuetest.In) queuetest.Out {
			queue = &Queue{}

			return queuetest.Out{
				Queue: queue,
				Enqueue: func(_ context.Context, envelopes ...*envelope.Envelope) {
					queue.Enqueue(envelopes...)
				},
			}
		},
		func() {
			queue.Close()
		},
	)
})

var _ = Describe("type Queue", func() {
	Describe("func Enqueue()", func() {
		It("panics if the queue is closed", func() {
			queue := &Queue{}
			queue.Close()

			Expect(func() {
				env := NewEnvelope("<id>", MessageA1)
				queue.Enqueue(env)
			}).To(Panic())
		})
	})
})
