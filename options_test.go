package infix

import (
	"time"

	"github.com/dogmatiq/dodeca/logging"
	"github.com/dogmatiq/linger/backoff"
	"github.com/dogmatiq/marshalkit/codec"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("func WithMessageTimeout()", func() {
	It("sets the message timeout", func() {
		opts := resolveOptions([]EngineOption{
			WithApplication(TestApplication),
			WithMessageTimeout(10 * time.Minute),
		})

		Expect(opts.MessageTimeout).To(Equal(10 * time.Minute))
	})

	It("uses the default if the duration is zero", func() {
		opts := resolveOptions([]EngineOption{
			WithApplication(TestApplication),
			WithMessageTimeout(0),
		})

		Expect(opts.MessageTimeout).To(Equal(DefaultMessageTimeout))
	})

	It("panics if the duration is less than zero", func() {
		Expect(func() {
			WithMessageTimeout(-1)
		}).To(Panic())
	})
})

var _ = Describe("func WithMessageBackoffStrategy()", func() {
	It("sets the backoff strategy", func() {
		p := backoff.Constant(10 * time.Second)

		opts := resolveOptions([]EngineOption{
			WithApplication(TestApplication),
			WithMessageBackoffStrategy(p),
		})

		Expect(opts.MessageBackoffStrategy(nil, 1)).To(Equal(10 * time.Second))
	})

	It("uses the default if the strategy is nil", func() {
		opts := resolveOptions([]EngineOption{
			WithApplication(TestApplication),
			WithMessageBackoffStrategy(nil),
		})

		Expect(opts.MessageBackoffStrategy).ToNot(BeNil())
	})
})

var _ = Describe("func WithMarshaler()", func() {
	It("sets the marshaler", func() {
		m := &codec.Marshaler{}

		opts := resolveOptions([]EngineOption{
			WithApplication(TestApplication),
			WithMarshaler(m),
		})

		Expect(opts.Marshaler).To(BeIdenticalTo(m))
	})

	It("constructs a default if the marshaler is nil", func() {
		opts := resolveOptions([]EngineOption{
			WithApplication(TestApplication),
			WithMarshaler(nil),
		})

		Expect(opts.Marshaler).To(Equal(
			NewDefaultMarshaler(opts.AppConfigs),
		))
	})
})

var _ = Describe("func WithLogger()", func() {
	It("sets the logger", func() {
		opts := resolveOptions([]EngineOption{
			WithApplication(TestApplication),
			WithLogger(logging.DebugLogger),
		})

		Expect(opts.Logger).To(BeIdenticalTo(logging.DebugLogger))
	})

	It("uses the default if the logger is nil", func() {
		opts := resolveOptions([]EngineOption{
			WithApplication(TestApplication),
			WithLogger(nil),
		})

		Expect(opts.Logger).To(Equal(DefaultLogger))
	})
})
