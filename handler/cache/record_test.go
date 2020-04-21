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

	Describe("func Release()", func() {
		var record *Record

		BeforeEach(func() {
			var err error
			record, err = cache.Acquire(ctx, "<id>")
			Expect(err).ShouldNot(HaveOccurred())

			record.Instance = "<value>"
		})

		It("removes the record from the cache by default", func() {
			record.Release()

			rec, err := cache.Acquire(ctx, "<id>")
			Expect(err).ShouldNot(HaveOccurred())
			defer rec.Release()

			// The instance will be nil again if the old record was not
			// retained.
			Expect(rec.Instance).To(BeNil())
		})

		It("keeps the record if KeepAlive() is called", func() {
			record.KeepAlive()
			record.Release()

			rec, err := cache.Acquire(ctx, "<id>")
			Expect(err).ShouldNot(HaveOccurred())
			defer rec.Release()

			Expect(rec.Instance).To(Equal("<value>"))
		})
	})
})
