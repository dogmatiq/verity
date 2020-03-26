package syncx_test

import (
	"context"
	"time"

	. "github.com/dogmatiq/infix/internal/x/syncx"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("type RWMutex", func() {
	var (
		ctx    context.Context
		cancel context.CancelFunc
		mutex  *RWMutex
	)

	BeforeEach(func() {
		ctx, cancel = context.WithTimeout(context.Background(), 10*time.Millisecond)
		mutex = &RWMutex{}
	})

	AfterEach(func() {
		cancel()
	})

	Describe("func Lock()", func() {
		It("blocks calls to Lock()", func() {
			err := mutex.Lock(ctx)
			Expect(err).ShouldNot(HaveOccurred())

			err = mutex.Lock(ctx)
			Expect(err).To(Equal(context.DeadlineExceeded))
		})

		It("blocks calls to RLock()", func() {
			err := mutex.Lock(ctx)
			Expect(err).ShouldNot(HaveOccurred())

			err = mutex.RLock(ctx)
			Expect(err).To(Equal(context.DeadlineExceeded))
		})
	})

	Describe("func Unlock()", func() {
		It("allows subsequent calls to Lock()", func() {
			err := mutex.Lock(ctx)
			Expect(err).ShouldNot(HaveOccurred())

			mutex.Unlock()

			err = mutex.Lock(ctx)
			Expect(err).ShouldNot(HaveOccurred())
		})

		It("allows subsequent calls to RLock()", func() {
			err := mutex.Lock(ctx)
			Expect(err).ShouldNot(HaveOccurred())

			mutex.Unlock()

			err = mutex.RLock(ctx)
			Expect(err).ShouldNot(HaveOccurred())
		})

		It("unblocks one blocking call to Lock()", func() {
			err := mutex.Lock(ctx)
			Expect(err).ShouldNot(HaveOccurred())

			errors := make(chan error, 2)
			fn := func() { errors <- mutex.Lock(ctx) }

			go fn()
			go fn()

			time.Sleep(5 * time.Millisecond)
			mutex.Unlock()

			Expect(<-errors).ShouldNot(HaveOccurred())
			Expect(<-errors).To(Equal(context.DeadlineExceeded))
		})

		It("unblocks all blocking calls to RLock()", func() {
			err := mutex.Lock(ctx)
			Expect(err).ShouldNot(HaveOccurred())

			errors := make(chan error, 2)
			fn := func() { errors <- mutex.RLock(ctx) }

			go fn()
			go fn()

			time.Sleep(5 * time.Millisecond)
			mutex.Unlock()

			Expect(<-errors).ShouldNot(HaveOccurred())
			Expect(<-errors).ShouldNot(HaveOccurred())
		})

		It("unblocks either one blocking call to Lock(), or all blocking calls to RLock()", func() {
			err := mutex.Lock(ctx)
			Expect(err).ShouldNot(HaveOccurred())

			errorsW := make(chan error, 4)
			fnW := func() { errorsW <- mutex.Lock(ctx) }
			go fnW()
			go fnW()

			errorsR := make(chan error, 4)
			fnR := func() { errorsR <- mutex.RLock(ctx) }
			go fnR()
			go fnR()

			time.Sleep(5 * time.Millisecond)
			mutex.Unlock()

			select {
			case err := <-errorsW:
				Expect(err).ShouldNot(HaveOccurred())

				// The mutex was Lock()'d, all other calls should timeout.
				Expect(<-errorsW).To(Equal(context.DeadlineExceeded))
				Expect(<-errorsR).To(Equal(context.DeadlineExceeded))
				Expect(<-errorsR).To(Equal(context.DeadlineExceeded))

			case err := <-errorsR:
				Expect(err).ShouldNot(HaveOccurred())

				// The mutex was RLock()'d, the other RLock() call should also
				// succeed.
				Expect(<-errorsR).ShouldNot(HaveOccurred())

				// And the Lock() calls sohuld timeout.
				Expect(<-errorsW).To(Equal(context.DeadlineExceeded))
				Expect(<-errorsW).To(Equal(context.DeadlineExceeded))

			}
		})

		It("panics if the mutex is not write-locked", func() {
			Expect(func() {
				mutex.Unlock()
			}).To(Panic())
		})
	})

	Describe("func RLock()", func() {
		It("blocks calls to Lock()", func() {
			err := mutex.RLock(ctx)
			Expect(err).ShouldNot(HaveOccurred())

			err = mutex.Lock(ctx)
			Expect(err).To(Equal(context.DeadlineExceeded))
		})

		It("does not block calls to RLock()", func() {
			err := mutex.RLock(ctx)
			Expect(err).ShouldNot(HaveOccurred())

			err = mutex.RLock(ctx)
			Expect(err).ShouldNot(HaveOccurred())
		})
	})

	Describe("func RUnlock()", func() {
		It("allows subsequent calls to Lock()", func() {
			err := mutex.RLock(ctx)
			Expect(err).ShouldNot(HaveOccurred())

			err = mutex.RLock(ctx)
			Expect(err).ShouldNot(HaveOccurred())

			mutex.RUnlock()
			mutex.RUnlock()

			err = mutex.Lock(ctx)
			Expect(err).ShouldNot(HaveOccurred())
		})

		It("does not allow subsequent calls to Lock() if other read-locks are still held", func() {
			err := mutex.RLock(ctx)
			Expect(err).ShouldNot(HaveOccurred())

			err = mutex.RLock(ctx)
			Expect(err).ShouldNot(HaveOccurred())

			mutex.RUnlock()

			err = mutex.Lock(ctx)
			Expect(err).To(Equal(context.DeadlineExceeded))
		})

		It("unblocks one blocking call to Lock()", func() {
			err := mutex.RLock(ctx)
			Expect(err).ShouldNot(HaveOccurred())

			err = mutex.RLock(ctx)
			Expect(err).ShouldNot(HaveOccurred())

			errors := make(chan error, 2)
			fn := func() { errors <- mutex.Lock(ctx) }

			go fn()
			go fn()

			time.Sleep(5 * time.Millisecond)
			mutex.RUnlock()
			mutex.RUnlock()

			Expect(<-errors).ShouldNot(HaveOccurred())
			Expect(<-errors).To(Equal(context.DeadlineExceeded))
		})

		It("does not unblock any calls to Lock() if other read-locks are still held", func() {
			err := mutex.RLock(ctx)
			Expect(err).ShouldNot(HaveOccurred())

			err = mutex.RLock(ctx)
			Expect(err).ShouldNot(HaveOccurred())

			errors := make(chan error, 2)
			fn := func() { errors <- mutex.Lock(ctx) }

			go fn()
			go fn()

			time.Sleep(5 * time.Millisecond)
			mutex.RUnlock()

			Expect(<-errors).To(Equal(context.DeadlineExceeded))
			Expect(<-errors).To(Equal(context.DeadlineExceeded))
		})

		It("panics if the mutex is not read-locked", func() {
			Expect(func() {
				mutex.RUnlock()
			}).To(Panic())
		})
	})
})
