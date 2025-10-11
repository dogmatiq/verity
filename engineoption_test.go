package verity

import (
	"time"

	//revive:disable:dot-imports
	"github.com/dogmatiq/configkit"
	"github.com/dogmatiq/dodeca/logging"
	"github.com/dogmatiq/dogma"
	. "github.com/dogmatiq/enginekit/enginetest/stubs"
	"github.com/dogmatiq/enginekit/marshaler"
	"github.com/dogmatiq/linger/backoff"
	. "github.com/dogmatiq/verity/fixtures"
	"github.com/dogmatiq/verity/persistence/memorypersistence"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var TestApplication = &ApplicationStub{
	ConfigureFunc: func(c dogma.ApplicationConfigurer) {
		c.Identity("<app-name>", DefaultAppKey)
		c.Routes(
			dogma.ViaProjection(&ProjectionMessageHandlerStub{
				ConfigureFunc: func(c dogma.ProjectionConfigurer) {
					c.Identity("<projection-name>", "b084ea4f-87d1-4001-8c1a-347c29baed35")
					c.Routes(
						dogma.HandlesEvent[*EventStub[TypeA]](),
					)
				},
			}),
		)
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
		p := &memorypersistence.Provider{}

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

var _ = Describe("func WithConcurrencyLimit()", func() {
	It("sets the concurrency limit", func() {
		opts := resolveEngineOptions(
			WithApplication(TestApplication),
			WithConcurrencyLimit(10),
		)

		Expect(opts.ConcurrencyLimit).To(BeEquivalentTo(10))
	})

	It("uses the default if the limit is zero", func() {
		opts := resolveEngineOptions(
			WithApplication(TestApplication),
			WithConcurrencyLimit(0),
		)

		Expect(opts.ConcurrencyLimit).To(Equal(DefaultConcurrencyLimit))
	})
})

var _ = Describe("func WithProjectionCompactInterval()", func() {
	It("sets the compaction interval", func() {
		opts := resolveEngineOptions(
			WithApplication(TestApplication),
			WithProjectionCompactInterval(10*time.Minute),
		)

		Expect(opts.ProjectionCompactInterval).To(Equal(10 * time.Minute))
	})

	It("uses the default if the duration is zero", func() {
		opts := resolveEngineOptions(
			WithApplication(TestApplication),
			WithProjectionCompactInterval(0),
		)

		Expect(opts.ProjectionCompactInterval).To(Equal(DefaultProjectionCompactInterval))
	})

	It("panics if the duration is less than zero", func() {
		Expect(func() {
			WithProjectionCompactInterval(-1)
		}).To(Panic())
	})
})

var _ = Describe("func WithProjectionCompactTimeout()", func() {
	It("sets the compaction timeout", func() {
		opts := resolveEngineOptions(
			WithApplication(TestApplication),
			WithProjectionCompactTimeout(10*time.Minute),
		)

		Expect(opts.ProjectionCompactTimeout).To(Equal(10 * time.Minute))
	})

	It("uses the default if the duration is zero", func() {
		opts := resolveEngineOptions(
			WithApplication(TestApplication),
			WithProjectionCompactTimeout(0),
		)

		Expect(opts.ProjectionCompactTimeout).To(Equal(DefaultProjectionCompactTimeout))
	})

	It("panics if the duration is less than zero", func() {
		Expect(func() {
			WithProjectionCompactTimeout(-1)
		}).To(Panic())
	})
})

var _ = Describe("func WithMarshaler()", func() {
	It("sets the marshaler", func() {
		m, err := marshaler.New(nil, nil)
		Expect(err).ShouldNot(HaveOccurred())

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
