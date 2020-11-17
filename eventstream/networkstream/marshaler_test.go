package networkstream_test

import (
	"github.com/dogmatiq/marshalkit"
	. "github.com/dogmatiq/verity/eventstream/networkstream"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("type NoopUnmarshaler", func() {
	Describe("func Unmarshal()", func() {
		It("returns nil", func() {
			v, err := NoopUnmarshaler{}.Unmarshal(marshalkit.Packet{})
			Expect(err).ShouldNot(HaveOccurred())
			Expect(v).To(BeNil())
		})
	})
})
