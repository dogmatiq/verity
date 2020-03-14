package memory_test

import (
	"context"

	"github.com/dogmatiq/configkit"
	. "github.com/dogmatiq/infix/persistence/provider/memory"
	. "github.com/dogmatiq/marshalkit/fixtures"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("type Provider", func() {
	var provider *Provider

	BeforeEach(func() {
		provider = &Provider{}
	})

	Describe("func Open()", func() {
		It("returns the same instance for each application", func() {
			ds1, err := provider.Open(
				context.Background(),
				configkit.MustNewIdentity("<app>", "<app-key>"),
				Marshaler,
			)
			Expect(err).ShouldNot(HaveOccurred())
			defer ds1.Close()

			ds2, err := provider.Open(
				context.Background(),
				configkit.MustNewIdentity("<app>", "<app-key>"),
				Marshaler,
			)
			Expect(err).ShouldNot(HaveOccurred())
			defer ds2.Close()

			Expect(ds1).To(BeIdenticalTo(ds2))
		})

		It("returns the different instances for different application", func() {
			ds1, err := provider.Open(
				context.Background(),
				configkit.MustNewIdentity("<app-1>", "<app-key-1>"),
				Marshaler,
			)
			Expect(err).ShouldNot(HaveOccurred())
			defer ds1.Close()

			ds2, err := provider.Open(
				context.Background(),
				configkit.MustNewIdentity("<app-2>", "<app-key-2>"),
				Marshaler,
			)
			Expect(err).ShouldNot(HaveOccurred())
			defer ds2.Close()

			Expect(ds1).ToNot(BeIdenticalTo(ds2))
		})
	})
})
