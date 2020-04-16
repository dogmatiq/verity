package envelopespec_test

import (
	"time"

	"github.com/dogmatiq/configkit"
	. "github.com/dogmatiq/dogma/fixtures"
	"github.com/dogmatiq/infix/draftspecs/envelopespec"
	. "github.com/dogmatiq/infix/draftspecs/envelopespec"
	. "github.com/dogmatiq/marshalkit/fixtures"
	. "github.com/jmalloc/gomegax"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("func MarshalMessage()", func() {
	It("marshals the message into the envelope", func() {
		var env envelopespec.Envelope

		MarshalMessage(
			Marshaler,
			MessageA1,
			&env,
		)

		Expect(&env).To(EqualX(
			&Envelope{
				PortableName: MessageAPortableName,
				MediaType:    MessageA1Packet.MediaType,
				Data:         MessageA1Packet.Data,
			},
		))
	})
})

var _ = Describe("func UnmarshalMessage()", func() {
	It("unmarshals the message from the envelope", func() {
		env := &Envelope{
			PortableName: MessageAPortableName,
			MediaType:    MessageA1Packet.MediaType,
			Data:         MessageA1Packet.Data,
		}

		m, err := UnmarshalMessage(Marshaler, env)
		Expect(err).ShouldNot(HaveOccurred())
		Expect(m).To(Equal(MessageA1))
	})
})

var _ = Describe("func MarshalIdentity()", func() {
	It("returns the protocol buffers identity", func() {
		in := configkit.MustNewIdentity("<name>", "<key>")

		out := MarshalIdentity(in)
		Expect(out).To(EqualX(
			&Identity{
				Name: "<name>",
				Key:  "<key>",
			},
		))
	})
})

var _ = Describe("func UnmarshalIdentity()", func() {
	It("returns the configkit identity", func() {
		in := &Identity{
			Name: "<name>",
			Key:  "<key>",
		}

		out, err := UnmarshalIdentity(in)
		Expect(err).ShouldNot(HaveOccurred())
		Expect(out).To(Equal(
			configkit.MustNewIdentity("<name>", "<key>"),
		))
	})
})

var _ = Describe("func MarshalTime()", func() {
	It("returns the time formatted as per RFC-3339", func() {
		in := time.Date(2001, 02, 03, 04, 05, 06, 0, time.UTC)

		out := MarshalTime(in)
		Expect(out).To(Equal("2001-02-03T04:05:06Z"))
	})

	It("returns an empty string if the time is the zero-value", func() {
		var in time.Time

		out := MarshalTime(in)
		Expect(out).To(Equal(""))
	})
})

var _ = Describe("func UnmarshalTime()", func() {
	It("parses the time from RFC-3339 format", func() {
		in := "2001-02-03T04:05:06Z"

		out, err := UnmarshalTime(in)
		Expect(err).ShouldNot(HaveOccurred())

		expect := time.Date(2001, 02, 03, 04, 05, 06, 0, time.UTC)
		Expect(out).To(BeTemporally("==", expect))
	})

	It("returns the zero-value if the input is empty", func() {
		out, err := UnmarshalTime("")
		Expect(err).ShouldNot(HaveOccurred())
		Expect(out.IsZero()).To(BeTrue())
	})
})
