package envelopespec_test

import (
	"time"

	"github.com/dogmatiq/configkit"
	. "github.com/dogmatiq/dogma/fixtures"
	"github.com/dogmatiq/infix/draftspecs/envelopespec"
	. "github.com/dogmatiq/infix/draftspecs/envelopespec"
	. "github.com/dogmatiq/infix/internal/x/gomegax"
	. "github.com/dogmatiq/marshalkit/fixtures"
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

		Expect(env).To(EqualX(
			Envelope{
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

// var _ = Describe("func Marshal()", func() {
// 	var in *envelope.Envelope

// 	BeforeEach(func() {
// 		in = NewEnvelope(
// 			"<id>",
// 			MessageA1,
// 			time.Now(),
// 			time.Now().Add(1*time.Hour),
// 		)
// 	})

// 	It("marshals to protobuf", func() {
// 		out, err := Marshal(Marshaler, in)
// 		Expect(err).ShouldNot(HaveOccurred())

// 		createdAt := in.CreatedAt.Format(time.RFC3339Nano)
// 		scheduledFor := in.ScheduledFor.Format(time.RFC3339Nano)

// 		Expect(out).To(Equal(&envelopespec.Envelope{
// 			MetaData: &envelopespec.MetaData{
// 				MessageId:     "<id>",
// 				CausationId:   "<cause>",
// 				CorrelationId: "<correlation>",
// 				Source: &envelopespec.Source{
// 					Application: &envelopespec.Identity{Name: "<app-name>", Key: "<app-key>"},
// 					Handler:     &envelopespec.Identity{Name: "<handler-name>", Key: "<handler-key>"},
// 					InstanceId:  "<instance>",
// 				},
// 				CreatedAt:    createdAt,
// 				ScheduledFor: scheduledFor,
// 				Description:  "{A1}",
// 			},
// 			PortableName: MessageAPortableName,
// 			MediaType:    MessageA1Packet.MediaType,
// 			Data:         MessageA1Packet.Data,
// 		}))
// 	})

// 	It("marshals a zero scheduled-for time as an empty string", func() {
// 		in.MetaData.ScheduledFor = time.Time{}

// 		out, err := Marshal(Marshaler, in)
// 		Expect(err).ShouldNot(HaveOccurred())
// 		Expect(out.MetaData.ScheduledFor).To(BeEmpty())
// 	})

// 	It("returns an error if marshaling fails", func() {
// 		in.Message = "<unsupported type>"

// 		_, err := Marshal(Marshaler, in)
// 		Expect(err).Should(HaveOccurred())
// 	})

// })

// var _ = Describe("func MustMarshal()", func() {
// 	It("marshals the envelope", func() {
// 		in := NewEnvelope("<id>", MessageA1)

// 		expect, err := Marshal(Marshaler, in)
// 		Expect(err).ShouldNot(HaveOccurred())

// 		out := MustMarshal(Marshaler, in)
// 		Expect(out).To(Equal(expect))
// 	})

// 	It("panics if marshaling fails", func() {
// 		in := NewEnvelope("<id>", MessageA1)
// 		in.Message = "<unsupported type>"

// 		Expect(func() {
// 			MustMarshal(Marshaler, in)
// 		}).To(Panic())
// 	})
// })

// var _ = Describe("func Unmarshal()", func() {
// 	var (
// 		createdAt, scheduledFor time.Time
// 		in                      *envelopespec.Envelope
// 	)

// 	BeforeEach(func() {
// 		createdAt = time.Now()
// 		scheduledFor = time.Now().Add(1 * time.Hour)

// 		in = MustMarshal(
// 			Marshaler,
// 			NewEnvelope(
// 				"<id>",
// 				MessageA1,
// 				createdAt,
// 				scheduledFor,
// 			),
// 		)
// 	})

// 	It("unmarshals from protobuf", func() {
// 		out, err := Unmarshal(Marshaler, in)
// 		Expect(err).ShouldNot(HaveOccurred())

// 		Expect(out.CreatedAt).To(BeTemporally("==", createdAt))
// 		Expect(out.ScheduledFor).To(BeTemporally("==", scheduledFor))

// 		// clear values for comparison below
// 		out.CreatedAt = time.Time{}
// 		out.ScheduledFor = time.Time{}

// 		Expect(out).To(Equal(&envelope.Envelope{
// 			MetaData: envelope.MetaData{
// 				MessageID:     "<id>",
// 				CausationID:   "<cause>",
// 				CorrelationID: "<correlation>",
// 				Source: envelope.Source{
// 					Application: configkit.MustNewIdentity("<app-name>", "<app-key>"),
// 					Handler:     configkit.MustNewIdentity("<handler-name>", "<handler-key>"),
// 					InstanceID:  "<instance>",
// 				},
// 			},
// 			Message: MessageA1,
// 		}))
// 	})

// 	It("returns an error if the created-at time can not be unmarshaled", func() {
// 		in.MetaData.CreatedAt = "not-a-valid-time"

// 		_, err := Unmarshal(Marshaler, in)
// 		Expect(err).Should(HaveOccurred())
// 	})

// 	It("returns an error if the scheduled-for time can not be unmarshaled", func() {
// 		in.MetaData.ScheduledFor = "not-a-valid-time"

// 		_, err := Unmarshal(Marshaler, in)
// 		Expect(err).Should(HaveOccurred())
// 	})

// 	It("does not return an error if the scheduled-for time is empty", func() {
// 		in.MetaData.ScheduledFor = ""

// 		out, err := Unmarshal(Marshaler, in)
// 		Expect(err).ShouldNot(HaveOccurred())
// 		Expect(out.MetaData.ScheduledFor.IsZero()).To(BeTrue())
// 	})

// 	It("returns an error if the source is not valid", func() {
// 		in.MetaData.Source.Handler = nil

// 		_, err := Unmarshal(Marshaler, in)
// 		Expect(err).Should(HaveOccurred())
// 	})

// 	It("returns an error if the meta-data is not valid", func() {
// 		in.MetaData.CreatedAt = ""

// 		_, err := Unmarshal(Marshaler, in)
// 		Expect(err).Should(HaveOccurred())
// 	})

// 	It("returns an error if the marshaler fails", func() {
// 		in.MediaType = "<unknown>"

// 		_, err := Unmarshal(Marshaler, in)
// 		Expect(err).Should(HaveOccurred())
// 	})
// })
