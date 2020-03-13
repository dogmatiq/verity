package memory_test

import (
	"context"

	"github.com/dogmatiq/configkit"
	. "github.com/dogmatiq/infix/persistence/provider/memory"
	. "github.com/dogmatiq/marshalkit/fixtures"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("type provider", func() {
	Describe("func Open()", func() {
		It("returns the same instance for each application", func() {
			p := New()

			ds1, err := p.Open(
				context.Background(),
				configkit.MustNewIdentity("<app>", "<app-key>"),
				Marshaler,
			)
			Expect(err).ShouldNot(HaveOccurred())
			defer ds1.Close()

			ds2, err := p.Open(
				context.Background(),
				configkit.MustNewIdentity("<app>", "<app-key>"),
				Marshaler,
			)
			Expect(err).ShouldNot(HaveOccurred())
			defer ds2.Close()

			Expect(ds1).To(BeIdenticalTo(ds2))
		})
	})
})
