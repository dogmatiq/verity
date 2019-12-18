package api

import (
	"github.com/dogmatiq/configkit"
	"github.com/dogmatiq/infix/api/internal/pb"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("func marshalIdentity()", func() {
	It("marshals to protobuf", func() {
		src := configkit.Identity{
			Name: "<name>",
			Key:  "<key>",
		}

		dest := marshalIdentity(src)

		Expect(dest).To(Equal(&pb.Identity{
			Name: "<name>",
			Key:  "<key>",
		}))
	})
})

var _ = Describe("func unmarshalIdentity()", func() {
	It("unmarshals from protobuf", func() {
		src := &pb.Identity{
			Name: "<name>",
			Key:  "<key>",
		}

		var dest configkit.Identity
		err := unmarshalIdentity(src, &dest)
		Expect(err).ShouldNot(HaveOccurred())

		Expect(dest).To(Equal(configkit.Identity{
			Name: "<name>",
			Key:  "<key>",
		}))
	})

	It("returns an error if the identity is invalid", func() {
		src := &pb.Identity{}

		var dest configkit.Identity
		err := unmarshalIdentity(src, &dest)
		Expect(err).Should(HaveOccurred())
	})
})
