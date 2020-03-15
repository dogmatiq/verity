package memory_test

import (
	"context"

	"github.com/dogmatiq/configkit"
	"github.com/dogmatiq/dogma"
	. "github.com/dogmatiq/dogma/fixtures"
	. "github.com/dogmatiq/infix/persistence/provider/memory"
	. "github.com/dogmatiq/marshalkit/fixtures"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("type Provider", func() {
	var (
		cfg1, cfg2 configkit.RichApplication
		provider   *Provider
	)

	BeforeEach(func() {
		cfg1 = configkit.FromApplication(&Application{
			ConfigureFunc: func(c dogma.ApplicationConfigurer) {
				c.Identity("<app-1>", "<app-key-1>")
			},
		})

		cfg2 = configkit.FromApplication(&Application{
			ConfigureFunc: func(c dogma.ApplicationConfigurer) {
				c.Identity("<app-2>", "<app-key-2>")
			},
		})

		provider = &Provider{}
	})

	Describe("func Open()", func() {
		It("returns the same instance for each application", func() {
			ds1, err := provider.Open(
				context.Background(),
				cfg1,
				Marshaler,
			)
			Expect(err).ShouldNot(HaveOccurred())
			defer ds1.Close()

			ds2, err := provider.Open(
				context.Background(),
				cfg1,
				Marshaler,
			)
			Expect(err).ShouldNot(HaveOccurred())
			defer ds2.Close()

			Expect(ds1).To(BeIdenticalTo(ds2))
		})

		It("returns the different instances for different application", func() {
			ds1, err := provider.Open(
				context.Background(),
				cfg1,
				Marshaler,
			)
			Expect(err).ShouldNot(HaveOccurred())
			defer ds1.Close()

			ds2, err := provider.Open(
				context.Background(),
				cfg2,
				Marshaler,
			)
			Expect(err).ShouldNot(HaveOccurred())
			defer ds2.Close()

			Expect(ds1).ToNot(BeIdenticalTo(ds2))
		})
	})
})
