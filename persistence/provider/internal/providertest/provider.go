package providertest

import (
	"context"

	"github.com/dogmatiq/infix/persistence"
	"github.com/onsi/ginkgo"
	"github.com/onsi/gomega"
)

func declareProviderTests(
	ctx *context.Context,
	in *In,
	out *Out,
) {
	ginkgo.Describe("type Provider (interface)", func() {
		ginkgo.Describe("func Open()", func() {
			ginkgo.It("returns different instances for different applications", func() {
				ds1, err := out.Provider.Open(*ctx, "<app-key-1>")
				gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
				defer ds1.Close()

				ds2, err := out.Provider.Open(*ctx, "<app-key-2>")
				gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
				defer ds2.Close()

				gomega.Expect(ds1).ToNot(gomega.BeIdenticalTo(ds2))
			})

			ginkgo.It("returns an error if the application's data-store is already open", func() {
				ds1, err := out.Provider.Open(*ctx, "<app-key>")
				gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
				defer ds1.Close()

				ds2, err := out.Provider.Open(*ctx, "<app-key>")
				if ds2 != nil {
					ds2.Close()
				}
				gomega.Expect(err).To(gomega.Equal(persistence.ErrDataStoreLocked))
			})

			ginkgo.It("allows re-opening a closed data-store", func() {
				ds, err := out.Provider.Open(*ctx, "<app-key>")
				gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
				ds.Close()

				ds, err = out.Provider.Open(*ctx, "<app-key>")
				gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
				ds.Close()
			})
		})
	})
}
