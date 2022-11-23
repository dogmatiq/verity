package providertest

import (
	"github.com/dogmatiq/verity/persistence"
	"github.com/onsi/ginkgo"
	"github.com/onsi/gomega"
)

func declareProviderTests(tc *TestContext) {
	ginkgo.Describe("type Provider (interface)", func() {
		var (
			provider      persistence.Provider
			closeProvider func()
		)

		ginkgo.BeforeEach(func() {
			provider, closeProvider = tc.Out.NewProvider()
		})

		ginkgo.AfterEach(func() {
			if closeProvider != nil {
				closeProvider()
			}
		})

		ginkgo.Describe("func Open()", func() {
			ginkgo.It("returns different instances for different applications", func() {
				ds1, err := provider.Open(tc.Context, "<app-key-1>")
				gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
				defer ds1.Close()

				ds2, err := provider.Open(tc.Context, "<app-key-2>")
				gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
				defer ds2.Close()

				gomega.Expect(ds1).ToNot(gomega.BeIdenticalTo(ds2))
			})

			ginkgo.It("allows re-opening a closed data-store", func() {
				ds, err := provider.Open(tc.Context, "<app-key>")
				gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
				ds.Close()

				ds, err = provider.Open(tc.Context, "<app-key>")
				gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
				ds.Close()
			})
		})
	})
}
