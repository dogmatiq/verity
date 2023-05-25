package providertest

import (
	"github.com/dogmatiq/verity/persistence"
	"github.com/onsi/ginkgo/v2"
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
				ds1, err := provider.Open(tc.Context, "a379e9f2-d324-4d2f-aff4-e8ae8fd6aa46")
				gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
				defer ds1.Close()

				ds2, err := provider.Open(tc.Context, "f853e8f1-43af-417c-9b90-aa4a8f5bd6ed")
				gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
				defer ds2.Close()

				gomega.Expect(ds1).ToNot(gomega.BeIdenticalTo(ds2))
			})

			ginkgo.It("allows re-opening a closed data-store", func() {
				ds, err := provider.Open(tc.Context, "d1fa6409-e581-4551-b43f-2824ba07298b")
				gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
				ds.Close()

				ds, err = provider.Open(tc.Context, "d1fa6409-e581-4551-b43f-2824ba07298b")
				gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
				ds.Close()
			})
		})
	})
}
