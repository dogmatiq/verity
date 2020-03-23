package providertest

import (
	"context"

	"github.com/dogmatiq/configkit"
	"github.com/dogmatiq/dogma"
	dogmafixtures "github.com/dogmatiq/dogma/fixtures"
	"github.com/onsi/ginkgo"
	"github.com/onsi/gomega"
)

func declareProviderTests(
	ctx *context.Context,
	in *In,
	out *Out,
) {
	ginkgo.Describe("type Provider (interface)", func() {
		app1 := configkit.FromApplication(&dogmafixtures.Application{
			ConfigureFunc: func(c dogma.ApplicationConfigurer) {
				c.Identity("<app1-name>", "<app1-key>")
			},
		})

		app2 := configkit.FromApplication(&dogmafixtures.Application{
			ConfigureFunc: func(c dogma.ApplicationConfigurer) {
				c.Identity("<app2-name>", "<app2-key>")
			},
		})

		ginkgo.Describe("func Open()", func() {
			ginkgo.It("allows repeat calls for the same application", func() {
				ds1, err := out.Provider.Open(*ctx, app1, in.Marshaler)
				gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
				defer ds1.Close()

				ds2, err := out.Provider.Open(*ctx, app1, in.Marshaler)
				gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
				defer ds2.Close()
			})

			ginkgo.It("returns different instances for different applications", func() {
				ds1, err := out.Provider.Open(*ctx, app1, in.Marshaler)
				gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
				defer ds1.Close()

				ds2, err := out.Provider.Open(*ctx, app2, in.Marshaler)
				gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
				defer ds2.Close()

				gomega.Expect(ds1).ToNot(gomega.BeIdenticalTo(ds2))
			})
		})
	})
}
