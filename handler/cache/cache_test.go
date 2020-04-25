package cache_test

import (
	"context"
	"time"

	"github.com/dogmatiq/dodeca/logging"
	"github.com/dogmatiq/infix/handler/cache"
	. "github.com/dogmatiq/infix/handler/cache"
	"github.com/dogmatiq/linger"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("type Cache", func() {
	var (
		ctx    context.Context
		cancel context.CancelFunc
		cache  *cache.Cache
	)

	BeforeEach(func() {
		ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
		cache = &Cache{
			TTL:    20 * time.Millisecond,
			Logger: logging.DebugLogger, // use a debug logger to ensure debug logging paths are covered
		}
	})

	AfterEach(func() {
		cancel()
	})

	Describe("func Acquire()", func() {
		When("the instance has no record in the cache", func() {
			It("returns a record with a nil instance", func() {
				rec, err := cache.Acquire(ctx, "<id>")
				Expect(err).ShouldNot(HaveOccurred())
				Expect(rec).NotTo(BeNil())
				defer rec.Release()

				Expect(rec.Instance).To(BeNil())
			})

			It("adds the record to the cache", func() {
				rec1, err := cache.Acquire(ctx, "<id>")
				Expect(err).ShouldNot(HaveOccurred())
				rec1.KeepAlive()
				rec1.Release()

				rec2, err := cache.Acquire(ctx, "<id>")
				Expect(err).ShouldNot(HaveOccurred())
				defer rec2.Release()

				Expect(rec1).To(BeIdenticalTo(rec2))
			})
		})

		When("the instance has a record in the cache", func() {
			var record *Record

			BeforeEach(func() {
				var err error
				record, err = cache.Acquire(ctx, "<id>")
				Expect(err).ShouldNot(HaveOccurred())

				record.Instance = "<value>"
			})

			When("the record is not locked", func() {
				BeforeEach(func() {
					record.KeepAlive()
					record.Release()
				})

				It("returns the same instance on subsequent invocations", func() {
					rec, err := cache.Acquire(ctx, "<id>")
					Expect(err).ShouldNot(HaveOccurred())
					defer rec.Release()

					Expect(rec.Instance).To(Equal("<value>"))
				})
			})

			When("the record is locked", func() {
				It("blocks until the record is released", func() {
					go func() {
						time.Sleep(10 * time.Millisecond)
						record.KeepAlive()
						record.Release()
					}()

					rec, err := cache.Acquire(ctx, "<id>")
					Expect(err).ShouldNot(HaveOccurred())
					defer rec.Release()

					Expect(rec.Instance).To(Equal("<value>"))
				})

				It("creates a new record if the locked record is not kept", func() {
					go func() {
						time.Sleep(10 * time.Millisecond)
						record.Release()
					}()

					rec, err := cache.Acquire(ctx, "<id>")
					Expect(err).ShouldNot(HaveOccurred())
					defer rec.Release()

					Expect(rec.Instance).To(BeNil())
				})

				It("returns an error if the deadline is exceeded", func() {
					ctx, cancel := context.WithTimeout(ctx, 20*time.Millisecond)
					defer cancel()

					rec, err := cache.Acquire(ctx, "<id>")
					if rec != nil {
						rec.Release()
					}
					Expect(err).To(Equal(context.DeadlineExceeded))
				})
			})
		})
	})

	Describe("func Run()", func() {
		It("evicts records that remain idle for two TTL periods", func() {
			By("adding a record")

			rec, err := cache.Acquire(ctx, "<id>")
			Expect(err).ShouldNot(HaveOccurred())
			rec.Instance = "<value>"
			rec.KeepAlive()
			rec.Release()

			By("running the eviction loop for longer than twice the TTL")

			runCtx, cancelRun := context.WithTimeout(ctx, 3*cache.TTL)
			defer cancelRun()
			err = cache.Run(runCtx)
			Expect(err).To(Equal(context.DeadlineExceeded))

			By("verifying that the value is not retained")

			rec, err = cache.Acquire(ctx, "<id>")
			Expect(err).ShouldNot(HaveOccurred())
			defer rec.Release()

			Expect(rec.Instance).To(BeNil())
		})

		It("does not evict records that are kept-alive after one TTL period", func() {
			runCtx, cancelRun := context.WithCancel(ctx)
			defer cancelRun()

			barrier := make(chan struct{})
			go func() {
				By("running the eviction loop in the background")
				barrier <- struct{}{}
				cache.Run(runCtx)
				barrier <- struct{}{}
			}()

			<-barrier
			By("adding a record")

			rec, err := cache.Acquire(ctx, "<id>")
			Expect(err).ShouldNot(HaveOccurred())
			rec.Instance = "<value>"
			rec.KeepAlive()
			rec.Release()

			By("waiting longer than one TTL period")

			time.Sleep(
				linger.Multiply(cache.TTL, 1.25),
			)

			By("acquiring the record and calling KeepAlive()")

			rec, err = cache.Acquire(ctx, "<id>")
			Expect(err).ShouldNot(HaveOccurred())
			rec.Instance = "<value>"
			rec.KeepAlive()
			rec.Release()

			By("waiting another TTL period")

			time.Sleep(cache.TTL)

			By("stopping the eviction loop")

			cancelRun()
			<-barrier

			By("verifying that the value was retained")

			rec, err = cache.Acquire(ctx, "<id>")
			Expect(err).ShouldNot(HaveOccurred())
			defer rec.Release()

			Expect(rec.Instance).To(Equal("<value>"))
		})

		It("does not evict locked records", func() {
			By("adding a record and holding the lock open")

			rec, err := cache.Acquire(ctx, "<id>")
			Expect(err).ShouldNot(HaveOccurred())
			rec.Instance = "<value>"

			By("running the eviction loop for longer than twice the TTL")

			runCtx, cancelRun := context.WithTimeout(ctx, 3*cache.TTL)
			defer cancelRun()
			err = cache.Run(runCtx)
			Expect(err).To(Equal(context.DeadlineExceeded))

			By("releasing the record only after the eviction loop has stopped")

			rec.KeepAlive()
			rec.Release()

			By("verifying that the value was retained")

			rec, err = cache.Acquire(ctx, "<id>")
			Expect(err).ShouldNot(HaveOccurred())
			defer rec.Release()

			Expect(rec.Instance).To(Equal("<value>"))
		})
	})
})
