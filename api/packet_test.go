package api

import (
	"github.com/dogmatiq/infix/api/internal/pb"
	"github.com/dogmatiq/marshalkit"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("func marshalPacket()", func() {
	It("marshals to protobuf", func() {
		src := marshalkit.Packet{
			MediaType: "<media-type>",
			Data:      []byte("<data>"),
		}

		dest := marshalPacket(src)

		Expect(dest).To(Equal(&pb.Packet{
			MediaType: "<media-type>",
			Data:      []byte("<data>"),
		}))
	})
})

var _ = Describe("func unmarshalPacket()", func() {
	It("unmarshals from protobuf", func() {
		src := &pb.Packet{
			MediaType: "<media-type>",
			Data:      []byte("<data>"),
		}

		var dest marshalkit.Packet
		unmarshalPacket(src, &dest)

		Expect(dest).To(Equal(marshalkit.Packet{
			MediaType: "<media-type>",
			Data:      []byte("<data>"),
		}))
	})
})
