package cache_test

import (
	"context"
	"time"

	"github.com/dogmatiq/infix/handler/cache"
	. "github.com/dogmatiq/infix/handler/cache"
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
		cache = &Cache{}
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

				record.Instance = "<instance value>"
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

					Expect(rec.Instance).To(Equal("<instance value>"))
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

					Expect(rec.Instance).To(Equal("<instance value>"))
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
})
