package memory_test

import (
	"context"

	"github.com/dogmatiq/configkit"
	"github.com/dogmatiq/infix/persistence"
	. "github.com/dogmatiq/infix/persistence/provider/memory"
	. "github.com/dogmatiq/marshalkit/fixtures"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("type dataStpre", func() {
	var dataStore persistence.DataStore

	BeforeEach(func() {
		provider := &Provider{}

		var err error
		dataStore, err = provider.Open(
			context.Background(),
			configkit.MustNewIdentity("<app>", "<app-key>"),
			Marshaler,
		)
		Expect(err).ShouldNot(HaveOccurred())
	})

	AfterEach(func() {
		dataStore.Close()
	})

	Describe("func EventStream()", func() {
		It("returns the same instance on subsequent calls", func() {
			stream1, err := dataStore.EventStream(context.Background())
			Expect(err).ShouldNot(HaveOccurred())

			stream2, err := dataStore.EventStream(context.Background())
			Expect(err).ShouldNot(HaveOccurred())

			Expect(stream1).To(BeIdenticalTo(stream2))
		})
	})
})
