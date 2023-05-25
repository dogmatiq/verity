package cache_test

import (
	"context"
	"errors"
	"time"

	"github.com/dogmatiq/dodeca/logging"
	"github.com/dogmatiq/linger"
	. "github.com/dogmatiq/verity/fixtures"
	"github.com/dogmatiq/verity/handler"
	"github.com/dogmatiq/verity/handler/cache"
	. "github.com/dogmatiq/verity/handler/cache"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("type Cache", func() {
	var (
		ctx   context.Context
		cache *cache.Cache
	)

	BeforeEach(func() {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
		DeferCleanup(cancel)

		cache = &Cache{
			TTL:    20 * time.Millisecond,
			Logger: logging.DebugLogger, // use a debug logger to ensure debug logging paths are covered
		}
	})

	Describe("func Acquire()", func() {
		When("the instance has no record in the cache", func() {
			It("returns a record with a nil instance", func() {
				rec, err := cache.Acquire(ctx, &UnitOfWorkStub{}, "<id>")
				Expect(err).ShouldNot(HaveOccurred())
				Expect(rec).NotTo(BeNil())
				Expect(rec.Instance).To(BeNil())
			})

			It("adds the record to the cache", func() {
				w := &UnitOfWorkStub{}
				rec1, err := cache.Acquire(ctx, w, "<id>")
				Expect(err).ShouldNot(HaveOccurred())
				w.Succeed(handler.Result{})

				rec2, err := cache.Acquire(ctx, &UnitOfWorkStub{}, "<id>")
				Expect(err).ShouldNot(HaveOccurred())

				Expect(rec1).To(BeIdenticalTo(rec2))
			})
		})

		When("the instance has a record in the cache", func() {
			var (
				record *Record
				work   *UnitOfWorkStub
			)

			BeforeEach(func() {
				work = &UnitOfWorkStub{}

				var err error
				record, err = cache.Acquire(ctx, work, "<id>")
				Expect(err).ShouldNot(HaveOccurred())
			})

			When("the record is not locked", func() {
				BeforeEach(func() {
					work.Succeed(handler.Result{})
				})

				It("returns the same record on subsequent invocations", func() {
					rec, err := cache.Acquire(ctx, &UnitOfWorkStub{}, "<id>")
					Expect(err).ShouldNot(HaveOccurred())
					Expect(rec).To(BeIdenticalTo(record))
				})
			})

			When("the record is locked", func() {
				It("blocks until the record is released", func() {
					go func() {
						time.Sleep(10 * time.Millisecond)
						work.Succeed(handler.Result{})
					}()

					rec, err := cache.Acquire(ctx, &UnitOfWorkStub{}, "<id>")
					Expect(err).ShouldNot(HaveOccurred())
					Expect(rec).To(BeIdenticalTo(record))
				})

				It("creates a new record if the locked record is not kept", func() {
					go func() {
						time.Sleep(10 * time.Millisecond)
						work.Fail(errors.New("<error>"))
					}()

					rec, err := cache.Acquire(ctx, &UnitOfWorkStub{}, "<id>")
					Expect(err).ShouldNot(HaveOccurred())
					Expect(rec).NotTo(BeIdenticalTo(record))
				})

				It("returns an error if the deadline is exceeded", func() {
					ctx, cancel := context.WithTimeout(ctx, 20*time.Millisecond)
					defer cancel()

					_, err := cache.Acquire(ctx, &UnitOfWorkStub{}, "<id>")
					Expect(err).To(Equal(context.DeadlineExceeded))
				})
			})
		})

		It("removes the record when the unit-of-work fails", func() {
			w := &UnitOfWorkStub{}
			rec1, err := cache.Acquire(ctx, w, "<id>")
			Expect(err).ShouldNot(HaveOccurred())

			w.Fail(errors.New("<error>"))

			rec2, err := cache.Acquire(ctx, &UnitOfWorkStub{}, "<id>")
			Expect(err).ShouldNot(HaveOccurred())

			Expect(rec2).NotTo(BeIdenticalTo(rec1))
		})
	})

	Describe("func Discard()", func() {
		It("removes the record from the cache even if the unit-of-work succeeds", func() {
			w := &UnitOfWorkStub{}
			rec1, err := cache.Acquire(ctx, w, "<id>")
			Expect(err).ShouldNot(HaveOccurred())

			cache.Discard(rec1)
			w.Succeed(handler.Result{})

			rec2, err := cache.Acquire(ctx, &UnitOfWorkStub{}, "<id>")
			Expect(err).ShouldNot(HaveOccurred())

			Expect(rec2).NotTo(BeIdenticalTo(rec1))
		})

		It("does not cause an error if the record is removed again because the unit-of-work fails", func() {
			w := &UnitOfWorkStub{}
			rec1, err := cache.Acquire(ctx, w, "<id>")
			Expect(err).ShouldNot(HaveOccurred())

			cache.Discard(rec1)
			w.Fail(errors.New("<error>"))

			rec2, err := cache.Acquire(ctx, &UnitOfWorkStub{}, "<id>")
			Expect(err).ShouldNot(HaveOccurred())

			Expect(rec2).NotTo(BeIdenticalTo(rec1))
		})
	})

	Describe("func Run()", func() {
		It("evicts records that remain idle for two TTL periods", func() {
			By("adding a record")

			w := &UnitOfWorkStub{}
			rec1, err := cache.Acquire(ctx, w, "<id>")
			Expect(err).ShouldNot(HaveOccurred())
			w.Succeed(handler.Result{})

			By("running the eviction loop for longer than twice the TTL")

			runCtx, cancelRun := context.WithTimeout(ctx, 3*cache.TTL)
			defer cancelRun()
			err = cache.Run(runCtx)
			Expect(err).To(Equal(context.DeadlineExceeded))

			By("verifying that the value is not retained")

			rec2, err := cache.Acquire(ctx, &UnitOfWorkStub{}, "<id>")
			Expect(err).ShouldNot(HaveOccurred())
			Expect(rec2).NotTo(BeIdenticalTo(rec1))
		})

		It("does not evict records that are used in a successful unit-of-work", func() {
			runCtx, cancelRun := context.WithCancel(ctx)
			defer cancelRun()

			barrier := make(chan struct{})
			go func() {
				defer GinkgoRecover()

				By("running the eviction loop in the background")

				barrier <- struct{}{}

				err := cache.Run(runCtx)
				Expect(err).To(Equal(context.Canceled))

				barrier <- struct{}{}
			}()

			<-barrier
			By("adding a record")

			w := &UnitOfWorkStub{}
			rec1, err := cache.Acquire(ctx, w, "<id>")
			Expect(err).ShouldNot(HaveOccurred())
			w.Succeed(handler.Result{})

			By("waiting longer than one TTL period")

			time.Sleep(
				linger.Multiply(cache.TTL, 1.25),
			)

			By("acquiring the record and releasing it again")

			w = &UnitOfWorkStub{}
			rec2, err := cache.Acquire(ctx, w, "<id>")
			Expect(err).ShouldNot(HaveOccurred())
			Expect(rec2).To(BeIdenticalTo(rec1))
			w.Succeed(handler.Result{})

			By("waiting another TTL period")

			time.Sleep(cache.TTL)

			By("stopping the eviction loop")

			cancelRun()
			<-barrier

			By("verifying that the value was retained")

			rec2, err = cache.Acquire(ctx, &UnitOfWorkStub{}, "<id>")
			Expect(err).ShouldNot(HaveOccurred())
			Expect(rec2).To(BeIdenticalTo(rec1))
		})

		It("does not evict locked records", func() {
			By("adding a record and holding the lock open")

			w := &UnitOfWorkStub{}
			rec1, err := cache.Acquire(ctx, w, "<id>")
			Expect(err).ShouldNot(HaveOccurred())

			By("running the eviction loop for longer than twice the TTL")

			runCtx, cancelRun := context.WithTimeout(ctx, 3*cache.TTL)
			defer cancelRun()
			err = cache.Run(runCtx)
			Expect(err).To(Equal(context.DeadlineExceeded))

			By("releasing the record only after the eviction loop has stopped")

			w.Succeed(handler.Result{})

			By("verifying that the value was retained")

			rec2, err := cache.Acquire(ctx, &UnitOfWorkStub{}, "<id>")
			Expect(err).ShouldNot(HaveOccurred())
			Expect(rec2).To(BeIdenticalTo(rec1))
		})
	})
})
