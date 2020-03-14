package sql

import (
	"time"

	"github.com/dogmatiq/linger/backoff"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("func WithMaxIdleConnections()", func() {
	It("sets the max idle count", func() {
		opts := resolveProviderOptions(
			WithMaxIdleConnections(5),
		)

		Expect(opts.MaxIdle).To(Equal(5))
	})

	It("uses the default if the limit is zero", func() {
		opts := resolveProviderOptions(
			WithMaxIdleConnections(0),
		)

		Expect(opts.MaxIdle).To(Equal(DefaultMaxIdleConnections))
	})
})

var _ = Describe("func WithMaxOpenConnections()", func() {
	It("sets the max open count", func() {
		opts := resolveProviderOptions(
			WithMaxOpenConnections(5),
		)

		Expect(opts.MaxOpen).To(Equal(5))
	})

	It("uses the default if the limit is zero", func() {
		opts := resolveProviderOptions(
			WithMaxOpenConnections(0),
		)

		Expect(opts.MaxOpen).To(Equal(DefaultMaxOpenConnections))
	})
})

var _ = Describe("func WithMaxLifetime()", func() {
	It("sets the max open count", func() {
		opts := resolveProviderOptions(
			WithMaxLifetime(5 * time.Minute),
		)

		Expect(opts.MaxLifetime).To(Equal(5 * time.Minute))
	})

	It("uses the default if the limit is zero", func() {
		opts := resolveProviderOptions(
			WithMaxLifetime(0),
		)

		Expect(opts.MaxLifetime).To(Equal(DefaultMaxLifetime))
	})
})

var _ = Describe("func WithStreamBackoff()", func() {
	It("sets the backoff strategy", func() {
		p := backoff.Constant(10 * time.Second)

		opts := resolveProviderOptions(
			WithStreamBackoff(p),
		)

		Expect(opts.StreamBackoff(nil, 1)).To(Equal(10 * time.Second))
	})

	It("uses the default if the strategy is nil", func() {
		opts := resolveProviderOptions(
			WithStreamBackoff(nil),
		)

		Expect(opts.StreamBackoff).ToNot(BeNil())
	})
})
