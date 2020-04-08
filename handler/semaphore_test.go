package handler_test

import (
	"context"
	"time"

	. "github.com/dogmatiq/infix/handler"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("type Semaphore", func() {
	When("the semaphore is uninitialized", func() {
		var sem Semaphore

		Describe("func Limit()", func() {
			It("returns zero", func() {
				Expect(sem.Limit()).To(Equal(0))
			})
		})

		Describe("func Acquire()", func() {
			It("does not return an error", func() {
				err := sem.Acquire(context.Background())
				Expect(err).ShouldNot(HaveOccurred())
				defer sem.Release()
			})
		})
	})

	When("the semaphore is initialized", func() {
		var sem Semaphore

		BeforeEach(func() {
			sem = NewSemaphore(1)
		})

		Describe("func Limit()", func() {
			It("returns the limit", func() {
				Expect(sem.Limit()).To(Equal(1))
			})
		})

		Describe("func Acquire()", func() {
			It("does not block if the limit has not been reached", func() {
				ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
				defer cancel()

				err := sem.Acquire(ctx)
				Expect(err).ShouldNot(HaveOccurred())
				defer sem.Release()
			})

			It("blocks if the limit has been reached", func() {
				ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
				defer cancel()

				err := sem.Acquire(ctx)
				Expect(err).ShouldNot(HaveOccurred())
				defer sem.Release()

				err = sem.Acquire(ctx)
				if err == nil {
					defer sem.Release()
				}
				Expect(err).To(Equal(context.DeadlineExceeded))
			})

			It("unblocks if Release() is called", func() {
				ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
				defer cancel()

				err := sem.Acquire(ctx)
				Expect(err).ShouldNot(HaveOccurred())

				go func() {
					time.Sleep(10 * time.Millisecond)
					sem.Release()
				}()

				err = sem.Acquire(ctx)
				Expect(err).ShouldNot(HaveOccurred())
				defer sem.Release()
			})
		})
	})
})
