package pipeline_test

import (
	"context"
	"time"

	"github.com/dogmatiq/infix/handler"
	. "github.com/dogmatiq/infix/pipeline"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("func LimitConcurrency()", func() {
	It("advances to the next stage if the semaphore is acquired", func() {
		sem := handler.NewSemaphore(1)

		stage := LimitConcurrency(sem)
		err := stage(context.Background(), &Scope{}, fail)
		Expect(err).To(MatchError("sink called"))
	})

	It("returns an error if the context is canceled while waiting for the semaphore", func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Millisecond)
		defer cancel()

		sem := handler.NewSemaphore(1)
		err := sem.Acquire(ctx)
		Expect(err).ShouldNot(HaveOccurred())
		defer sem.Release()

		stage := LimitConcurrency(sem)
		err = stage(ctx, &Scope{}, fail)
		Expect(err).To(Equal(context.DeadlineExceeded))
	})

})
