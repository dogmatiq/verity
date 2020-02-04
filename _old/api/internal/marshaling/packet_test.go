package marshaling_test

import (
	. "github.com/dogmatiq/infix/api/internal/marshaling"
	"github.com/dogmatiq/infix/api/internal/pb"
	"github.com/dogmatiq/marshalkit"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("func MarshalPacket()", func() {
	It("marshals to protobuf", func() {
		src := marshalkit.Packet{
			MediaType: "<media-type>",
			Data:      []byte("<data>"),
		}

		dest := MarshalPacket(src)

		Expect(dest).To(Equal(&pb.Packet{
			MediaType: "<media-type>",
			Data:      []byte("<data>"),
		}))
	})
})

var _ = Describe("func UnmarshalPacket()", func() {
	It("unmarshals from protobuf", func() {
		src := &pb.Packet{
			MediaType: "<media-type>",
			Data:      []byte("<data>"),
		}

		var dest marshalkit.Packet
		UnmarshalPacket(src, &dest)

		Expect(dest).To(Equal(marshalkit.Packet{
			MediaType: "<media-type>",
			Data:      []byte("<data>"),
		}))
	})
})
