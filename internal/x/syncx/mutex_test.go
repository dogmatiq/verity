package syncx_test

import (
	"context"
	"fmt"
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
		ctx, cancel = context.WithTimeout(context.Background(), 50*time.Millisecond)
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

		It("returns an error if the context is canceled", func() {
			// The idea here is to bail before ever trying to acquire the
			// internal mutex.
			cancel()

			err := mutex.Lock(ctx)
			Expect(err).To(Equal(context.Canceled))
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

				// And the Lock() calls should timeout.
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

		It("returns an error if the context is canceled", func() {
			// The idea here is to bail before ever trying to acquire the
			// internal mutex.
			cancel()

			err := mutex.RLock(ctx)
			Expect(err).To(Equal(context.Canceled))
		})

		It("does not panic if two calls succeed without retries", func() {
			// This is a regression test for
			// https://github.com/dogmatiq/infix/issues/72.
			//
			// This issue occurs when a call to RUnlock() is made AFTER an
			// RLock() call has taken function-local copies of the internal
			// channels, but BEFORE it starts selecting from them.
			//
			// In this case, by the time the second RLock() starts selecting
			// BOTH the `ready` and `unlocked` channels are ready for reading,
			// and it's undefined which case of the select statement will
			// execute.
			//
			// If the `<-unlocked` case is executed, a panic occurs when
			// attempting to `close(m.ready)` because it has already been set to
			// `nil` by the the unlocking goroutine's initial RLock() call.

			concurrency := 200
			errors := make(chan error, concurrency)

			for i := 0; i < concurrency; i++ {
				go func() {
					defer func() {
						if v := recover(); v != nil {
							errors <- fmt.Errorf("panic: %s", v)
						}
					}()

					err := mutex.RLock(ctx)
					if err == nil {
						mutex.RUnlock()
					}

					errors <- err
				}()
			}

			// When the panic does occur, other calls to RLock hang forever, so
			// we need to fail the test the moment we get a panic.
			//
			// The other goroutines will remain blocked in the background until
			// the test runner exists, but at least this way we get meaningful
			// output, rather than a panic midway through the test suite.
			n := 0
			for n < concurrency {
				select {
				case err := <-errors:
					Expect(err).ShouldNot(HaveOccurred())
				case <-ctx.Done():
					Expect(ctx.Err()).ShouldNot(HaveOccurred())
				}

				n++
			}
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
