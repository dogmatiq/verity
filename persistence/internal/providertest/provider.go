package providertest

import (
	"github.com/dogmatiq/infix/persistence"
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

			ginkgo.It("returns an error if the application's data-store is already open", func() {
				ds1, err := provider.Open(tc.Context, "<app-key>")
				gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
				defer ds1.Close()

				ds2, err := provider.Open(tc.Context, "<app-key>")
				if ds2 != nil {
					ds2.Close()
				}
				gomega.Expect(err).To(gomega.Equal(persistence.ErrDataStoreLocked))
			})

			ginkgo.It("allows re-opening a closed data-store", func() {
				ds, err := provider.Open(tc.Context, "<app-key>")
				gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
				ds.Close()

				ds, err = provider.Open(tc.Context, "<app-key>")
				gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
				ds.Close()
			})

			ginkgo.When("the provider shares data across instances", func() {
				ginkgo.BeforeEach(func() {
					if !tc.Out.IsShared {
						ginkgo.Skip("provider does not share data across instances")
					}
				})

				ginkgo.It("returns an error if the application's data-store has already been opened on another instance", func() {
					p, c := tc.Out.NewProvider()
					if c != nil {
						defer c()
					}

					ds1, err := provider.Open(tc.Context, "<app-key>")
					gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
					defer ds1.Close()

					ds2, err := p.Open(tc.Context, "<app-key>")
					if ds2 != nil {
						ds2.Close()
					}
					gomega.Expect(err).To(gomega.Equal(persistence.ErrDataStoreLocked))
				})
			})
		})
	})
}
