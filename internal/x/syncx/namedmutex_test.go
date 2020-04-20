package syncx_test

import (
	"context"
	"time"

	. "github.com/dogmatiq/infix/internal/x/syncx"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("type MutexNamespace", func() {
	var (
		ctx    context.Context
		cancel context.CancelFunc
		ns     *MutexNamespace
	)

	BeforeEach(func() {
		ctx, cancel = context.WithTimeout(context.Background(), 20*time.Millisecond)
		ns = &MutexNamespace{}
	})

	AfterEach(func() {
		cancel()
	})

	Describe("func Lock()", func() {
		It("returns an unlock function", func() {
			u, err := ns.Lock(ctx, "<name>")
			Expect(err).ShouldNot(HaveOccurred())
			Expect(u).NotTo(BeNil())
			u()
		})

		It("allows re-locking of the same mutex", func() {
			u, err := ns.Lock(ctx, "<name>")
			Expect(err).ShouldNot(HaveOccurred())
			u()

			u, err = ns.Lock(ctx, "<name>")
			Expect(err).ShouldNot(HaveOccurred())
			u()
		})

		It("allows locking of two different mutexes", func() {
			u, err := ns.Lock(ctx, "<name-1>")
			Expect(err).ShouldNot(HaveOccurred())
			defer u()

			u, err = ns.Lock(ctx, "<name-2>")
			Expect(err).ShouldNot(HaveOccurred())
			u()
		})

		When("the mutex is already locked", func() {
			var unlock UnlockFunc

			BeforeEach(func() {
				var err error
				unlock, err = ns.Lock(ctx, "<name>")
				Expect(err).ShouldNot(HaveOccurred())
			})

			AfterEach(func() {
				unlock()
			})

			It("blocks until the mutex is unlocked", func() {
				go func() {
					time.Sleep(20 * time.Millisecond)
					unlock()
				}()

				u, err := ns.Lock(ctx, "<name>")
				Expect(err).ShouldNot(HaveOccurred())
				u()
			})

			It("returns an error if the deadline is exceeded", func() {
				ctx, cancel := context.WithTimeout(ctx, 20*time.Millisecond)
				defer cancel()

				u, err := ns.Lock(ctx, "<name>")
				if u != nil {
					u()
				}
				Expect(err).To(Equal(context.DeadlineExceeded))
			})
		})
	})
})
