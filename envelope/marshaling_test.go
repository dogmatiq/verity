package envelope_test

import (
	"time"

	"github.com/dogmatiq/configkit"
	. "github.com/dogmatiq/dogma/fixtures"
	"github.com/dogmatiq/infix/envelope"
	. "github.com/dogmatiq/infix/envelope"
	. "github.com/dogmatiq/infix/fixtures"
	"github.com/dogmatiq/infix/internal/draftspecs/envelopespec"
	"github.com/dogmatiq/marshalkit"
	. "github.com/dogmatiq/marshalkit/fixtures"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("func Marshal()", func() {
	var in *envelope.Envelope

	BeforeEach(func() {
		in = &envelope.Envelope{
			MetaData: envelope.MetaData{
				MessageID:     "<id>",
				CausationID:   "<cause>",
				CorrelationID: "<correlation>",
				Source: envelope.Source{
					Application: configkit.MustNewIdentity("<app-name>", "<app-key>"),
					Handler:     configkit.MustNewIdentity("<handler-name>", "<handler-key>"),
					InstanceID:  "<instance>",
				},
				CreatedAt:    time.Now(),
				ScheduledFor: time.Now().Add(1 * time.Hour),
			},
			Message: MessageA1,
			Packet: marshalkit.Packet{
				MediaType: MessageA1Packet.MediaType,
				Data:      MessageA1Packet.Data,
			},
		}
	})

	It("marshals to protobuf", func() {
		out, err := Marshal(in)
		Expect(err).ShouldNot(HaveOccurred())

		createdAt, err := in.CreatedAt.MarshalBinary()
		Expect(err).ShouldNot(HaveOccurred())

		scheduledFor, err := in.ScheduledFor.MarshalBinary()
		Expect(err).ShouldNot(HaveOccurred())

		Expect(out).To(Equal(&envelopespec.Envelope{
			MetaData: &envelopespec.MetaData{
				MessageId:     "<id>",
				CausationId:   "<cause>",
				CorrelationId: "<correlation>",
				Source: &envelopespec.Source{
					Application: &envelopespec.Identity{Name: "<app-name>", Key: "<app-key>"},
					Handler:     &envelopespec.Identity{Name: "<handler-name>", Key: "<handler-key>"},
					InstanceId:  "<instance>",
				},
				CreatedAt:    createdAt,
				ScheduledFor: scheduledFor,
			},
			MediaType: MessageA1Packet.MediaType,
			Data:      MessageA1Packet.Data,
		}))
	})

	It("marshals a zero scheduled-for time as an empty buffer", func() {
		in.MetaData.ScheduledFor = time.Time{}

		out, err := Marshal(in)
		Expect(err).ShouldNot(HaveOccurred())
		Expect(out.MetaData.ScheduledFor).To(BeNil())
	})

	It("returns an error if the created-at time cannot be marshaled", func() {
		in.MetaData.CreatedAt = time.Now().In(
			time.FixedZone("fractional", 30),
		)

		_, err := Marshal(in)
		Expect(err).Should(HaveOccurred())
	})

	It("returns an error if the scheduled-for time cannot be marshaled", func() {
		in.MetaData.ScheduledFor = time.Now().In(
			time.FixedZone("fractional", 30),
		)

		_, err := Marshal(in)
		Expect(err).Should(HaveOccurred())
	})
})

var _ = Describe("func MustMarshal()", func() {
	It("marshals the envelope", func() {
		in := NewEnvelope("<id>", MessageA1)

		expect, err := Marshal(in)
		Expect(err).ShouldNot(HaveOccurred())

		out := MustMarshal(in)
		Expect(out).To(Equal(expect))
	})

	It("panics if marshaling fails", func() {
		in := NewEnvelope("<id>", MessageA1)
		in.MetaData.ScheduledFor = time.Now().In(
			time.FixedZone("fractional", 30),
		)

		Expect(func() {
			MustMarshal(in)
		}).To(Panic())
	})
})

var _ = Describe("func Unmarshal()", func() {
	var (
		createdAt, scheduledFor time.Time
		in                      *envelopespec.Envelope
	)

	BeforeEach(func() {
		createdAt = time.Now()
		scheduledFor = createdAt.Add(1 * time.Hour)

		createdAtData, err := createdAt.MarshalBinary()
		Expect(err).ShouldNot(HaveOccurred())

		scheduledForData, err := scheduledFor.MarshalBinary()
		Expect(err).ShouldNot(HaveOccurred())

		in = &envelopespec.Envelope{
			MetaData: &envelopespec.MetaData{
				MessageId:     "<id>",
				CausationId:   "<cause>",
				CorrelationId: "<correlation>",
				Source: &envelopespec.Source{
					Application: &envelopespec.Identity{Name: "<app-name>", Key: "<app-key>"},
					Handler:     &envelopespec.Identity{Name: "<handler-name>", Key: "<handler-key>"},
					InstanceId:  "<instance>",
				},
				CreatedAt:    createdAtData,
				ScheduledFor: scheduledForData,
			},
			MediaType: MessageA1Packet.MediaType,
			Data:      MessageA1Packet.Data,
		}
	})

	It("unmarshals from protobuf", func() {
		var out envelope.Envelope
		err := Unmarshal(Marshaler, in, &out)
		Expect(err).ShouldNot(HaveOccurred())

		Expect(out.CreatedAt).To(BeTemporally("==", createdAt))
		Expect(out.ScheduledFor).To(BeTemporally("==", scheduledFor))

		// clear values for comparison below
		out.CreatedAt = time.Time{}
		out.ScheduledFor = time.Time{}

		Expect(out).To(Equal(envelope.Envelope{
			MetaData: envelope.MetaData{
				MessageID:     "<id>",
				CausationID:   "<cause>",
				CorrelationID: "<correlation>",
				Source: envelope.Source{
					Application: configkit.MustNewIdentity("<app-name>", "<app-key>"),
					Handler:     configkit.MustNewIdentity("<handler-name>", "<handler-key>"),
					InstanceID:  "<instance>",
				},
			},
			Message: MessageA1,
			Packet:  MessageA1Packet,
		}))
	})

	It("returns an error if the created-at time can not be unmarshaled", func() {
		in.MetaData.CreatedAt = []byte("not-a-valid-time")

		var out envelope.Envelope
		err := Unmarshal(Marshaler, in, &out)
		Expect(err).Should(HaveOccurred())
	})

	It("returns an error if the scheduled-for time can not be unmarshaled", func() {
		in.MetaData.ScheduledFor = []byte("not-a-valid-time")

		var out envelope.Envelope
		err := Unmarshal(Marshaler, in, &out)
		Expect(err).Should(HaveOccurred())
	})

	It("does not return an error if the scheduled-for time is empty", func() {
		in.MetaData.ScheduledFor = nil

		var out envelope.Envelope
		err := Unmarshal(Marshaler, in, &out)
		Expect(err).ShouldNot(HaveOccurred())
		Expect(out.MetaData.ScheduledFor.IsZero()).To(BeTrue())
	})

	It("returns an error if the source is not valid", func() {
		in.MetaData.Source.Handler = nil

		var out envelope.Envelope
		err := Unmarshal(Marshaler, in, &out)
		Expect(err).Should(HaveOccurred())
	})

	It("returns an error if the meta-data is not valid", func() {
		in.MetaData.CreatedAt = nil

		var out envelope.Envelope
		err := Unmarshal(Marshaler, in, &out)
		Expect(err).Should(HaveOccurred())
	})

	It("returns an error if the marshaler fails", func() {
		in.MediaType = "<unknown>"

		var out envelope.Envelope
		err := Unmarshal(Marshaler, in, &out)
		Expect(err).Should(HaveOccurred())
	})
})
