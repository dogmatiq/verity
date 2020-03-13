package infix

import (
	"github.com/dogmatiq/configkit"
	"github.com/dogmatiq/dogma"
	. "github.com/dogmatiq/dogma/fixtures"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var TestApplication = &Application{
	ConfigureFunc: func(c dogma.ApplicationConfigurer) {
		c.Identity("<app-name>", "<app-key>")
	},
}

var _ = Describe("func WithApplication()", func() {
	It("adds the application", func() {
		opts := resolveOptions([]EngineOption{
			WithApplication(TestApplication),
		})

		Expect(opts.AppConfigs).To(ConsistOf(
			configkit.FromApplication(TestApplication),
		))
	})

	It("panics if the none are provided", func() {
		Expect(func() {
			resolveOptions(nil)
		}).To(Panic())
	})
})
