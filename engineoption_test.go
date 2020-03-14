package infix

import (
	"time"

	"github.com/dogmatiq/configkit"
	"github.com/dogmatiq/dodeca/logging"
	"github.com/dogmatiq/dogma"
	. "github.com/dogmatiq/dogma/fixtures"
	"github.com/dogmatiq/infix/persistence/provider/memory"
	"github.com/dogmatiq/linger/backoff"
	"github.com/dogmatiq/marshalkit/codec"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var TestApplication = &Application{
	ConfigureFunc: func(c dogma.ApplicationConfigurer) {
		c.Identity("<app-name>", "<app-key>")
		c.RegisterProjection(&ProjectionMessageHandler{
			ConfigureFunc: func(c dogma.ProjectionConfigurer) {
				c.Identity("<projection-name>", "<projection-key>")
				c.ConsumesEventType(MessageA{})
			},
		})
	},
}

var _ = Describe("func WithApplication()", func() {
	It("adds the application", func() {
		opts := resolveEngineOptions(
			WithApplication(TestApplication),
		)

		Expect(opts.AppConfigs).To(HaveLen(1))
		Expect(
			configkit.IsApplicationEqual(
				opts.AppConfigs[0],
				configkit.FromApplication(TestApplication),
			),
		).To(BeTrue())
	})

	It("panics if application identities conflict", func() {
		Expect(func() {
			resolveEngineOptions(
				WithApplication(TestApplication),
				WithApplication(TestApplication),
			)
		}).To(Panic())
	})

	It("panics if no WithApplication() options are provided", func() {
		Expect(func() {
			resolveEngineOptions(
				WithMessageTimeout(1 * time.Second), // provide something else
			)
		}).To(Panic())
	})
})

var _ = Describe("func WithPersistence()", func() {
	It("sets the persistence provider", func() {
		p := &memory.Provider{}

		opts := resolveEngineOptions(
			WithApplication(TestApplication),
			WithPersistence(p),
		)

		Expect(opts.PersistenceProvider).To(Equal(p))
	})

	It("uses the default if the provider is nil", func() {
		opts := resolveEngineOptions(
			WithApplication(TestApplication),
			WithPersistence(nil),
		)

		Expect(opts.PersistenceProvider).To(Equal(DefaultPersistenceProvider))
	})
})

var _ = Describe("func WithMessageTimeout()", func() {
	It("sets the message timeout", func() {
		opts := resolveEngineOptions(
			WithApplication(TestApplication),
			WithMessageTimeout(10*time.Minute),
		)

		Expect(opts.MessageTimeout).To(Equal(10 * time.Minute))
	})

	It("uses the default if the duration is zero", func() {
		opts := resolveEngineOptions(
			WithApplication(TestApplication),
			WithMessageTimeout(0),
		)

		Expect(opts.MessageTimeout).To(Equal(DefaultMessageTimeout))
	})

	It("panics if the duration is less than zero", func() {
		Expect(func() {
			WithMessageTimeout(-1)
		}).To(Panic())
	})
})

var _ = Describe("func WithMessageBackoff()", func() {
	It("sets the backoff strategy", func() {
		p := backoff.Constant(10 * time.Second)

		opts := resolveEngineOptions(
			WithApplication(TestApplication),
			WithMessageBackoff(p),
		)

		Expect(opts.MessageBackoff(nil, 1)).To(Equal(10 * time.Second))
	})

	It("uses the default if the strategy is nil", func() {
		opts := resolveEngineOptions(
			WithApplication(TestApplication),
			WithMessageBackoff(nil),
		)

		Expect(opts.MessageBackoff).ToNot(BeNil())
	})
})

var _ = Describe("func WithMarshaler()", func() {
	It("sets the marshaler", func() {
		m := &codec.Marshaler{}

		opts := resolveEngineOptions(
			WithApplication(TestApplication),
			WithMarshaler(m),
		)

		Expect(opts.Marshaler).To(BeIdenticalTo(m))
	})

	It("constructs a default if the marshaler is nil", func() {
		opts := resolveEngineOptions(
			WithApplication(TestApplication),
			WithMarshaler(nil),
		)

		Expect(opts.Marshaler).To(Equal(
			NewDefaultMarshaler(opts.AppConfigs),
		))
	})
})

var _ = Describe("func WithLogger()", func() {
	It("sets the logger", func() {
		opts := resolveEngineOptions(
			WithApplication(TestApplication),
			WithLogger(logging.DebugLogger),
		)

		Expect(opts.Logger).To(BeIdenticalTo(logging.DebugLogger))
	})

	It("uses the default if the logger is nil", func() {
		opts := resolveEngineOptions(
			WithApplication(TestApplication),
			WithLogger(nil),
		)

		Expect(opts.Logger).To(Equal(DefaultLogger))
	})
})
