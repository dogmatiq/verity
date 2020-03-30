package envelope_test

import (
	"time"

	. "github.com/dogmatiq/dogma/fixtures"
	"github.com/dogmatiq/infix/envelope"
	. "github.com/dogmatiq/infix/envelope"
	. "github.com/dogmatiq/infix/fixtures"
	. "github.com/dogmatiq/marshalkit/fixtures"
	"github.com/golang/protobuf/proto"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("func MarshalBinary(), MustMarshalBinary() and UnmarshalBinary()", func() {
	var in *envelope.Envelope

	BeforeEach(func() {
		in = NewEnvelope(
			"<id>",
			MessageA1,
			time.Now(),
			time.Now().Add(1*time.Hour),
		)
	})

	Describe("func MarshalBinary()", func() {
		It("marshals and unmarshals", func() {
			data, err := MarshalBinary(in)
			Expect(err).ShouldNot(HaveOccurred())

			var out Envelope
			err = UnmarshalBinary(Marshaler, data, &out)
			Expect(err).ShouldNot(HaveOccurred())
			Expect(&out).To(Equal(in))
		})

		It("returns an error if marshaling fails", func() {
			in.Packet.MediaType = "<malformed>"

			_, err := MarshalBinary(in)
			Expect(err).Should(HaveOccurred())
		})
	})

	Describe("func UnmarshalBinary()", func() {
		It("returns an error if the data is not valid", func() {
			var out Envelope
			err := UnmarshalBinary(Marshaler, []byte("<nonsense>"), &out)
			Expect(err).Should(HaveOccurred())
		})

		It("returns an error if the data contains a malformed envelope", func() {
			pb := MustMarshal(in)      // marshal to protobuf
			pb.MetaData.MessageId = "" // make it invalid
			data, err := proto.Marshal(pb)
			Expect(err).ShouldNot(HaveOccurred())

			var out Envelope
			err = UnmarshalBinary(Marshaler, data, &out)
			Expect(err).Should(HaveOccurred())
		})
	})

	Describe("func MustMarshalBinary()", func() {
		It("marshals and unmarshals", func() {
			data := MustMarshalBinary(in)

			var out Envelope
			err := UnmarshalBinary(Marshaler, data, &out)
			Expect(err).ShouldNot(HaveOccurred())
			Expect(&out).To(Equal(in))
		})

		It("panics if marshaling fails", func() {
			in.Packet.MediaType = "<malformed>"

			Expect(func() {
				MustMarshalBinary(in)
			}).To(Panic())
		})
	})
})
