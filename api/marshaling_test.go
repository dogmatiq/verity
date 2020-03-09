package api

import (
	"time"

	"github.com/dogmatiq/configkit"
	. "github.com/dogmatiq/dogma/fixtures"
	"github.com/dogmatiq/infix/api/internal/pb"
	"github.com/dogmatiq/infix/envelope"
	"github.com/dogmatiq/marshalkit"
	. "github.com/dogmatiq/marshalkit/fixtures"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("func marshalIdentity()", func() {
	It("returns the protobuf representation", func() {
		in := configkit.MustNewIdentity("<name>", "<key>")

		out := marshalIdentity(in)

		Expect(out).To(Equal(&pb.Identity{
			Name: "<name>",
			Key:  "<key>",
		}))
	})
})

var _ = Describe("func unmarshalIdentity()", func() {
	It("returns the native representation", func() {
		in := &pb.Identity{
			Name: "<name>",
			Key:  "<key>",
		}

		var out configkit.Identity
		err := unmarshalIdentity(in, &out)

		Expect(err).ShouldNot(HaveOccurred())
		Expect(out).To(Equal(
			configkit.MustNewIdentity("<name>", "<key>"),
		))
	})

	It("returns an error if the identity is invalid", func() {
		in := &pb.Identity{}

		var out configkit.Identity
		err := unmarshalIdentity(in, &out)

		Expect(err).Should(HaveOccurred())
	})
})

var _ = Describe("func marshalEnvelope()", func() {
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
		out := marshalEnvelope(in)

		createdAt, err := in.CreatedAt.MarshalBinary()
		Expect(err).ShouldNot(HaveOccurred())

		scheduledFor, err := in.ScheduledFor.MarshalBinary()
		Expect(err).ShouldNot(HaveOccurred())

		Expect(out).To(Equal(&pb.Envelope{
			MetaData: &pb.MetaData{
				MessageId:     "<id>",
				CausationId:   "<cause>",
				CorrelationId: "<correlation>",
				Source: &pb.Source{
					Application: &pb.Identity{Name: "<app-name>", Key: "<app-key>"},
					Handler:     &pb.Identity{Name: "<handler-name>", Key: "<handler-key>"},
					InstanceId:  "<instance>",
				},
				CreatedAt:    createdAt,
				ScheduledFor: scheduledFor,
			},
			Packet: &pb.Packet{
				MediaType: MessageA1Packet.MediaType,
				Data:      MessageA1Packet.Data,
			},
		}))
	})

	It("marshals a zero scheduled-for time as an empty buffer", func() {
		in.MetaData.ScheduledFor = time.Time{}

		out := marshalEnvelope(in)

		Expect(out.MetaData.ScheduledFor).To(BeNil())
	})

	It("panics if a time-value cannot be marshaled", func() {
		in.MetaData.CreatedAt = time.Now().In(
			time.FixedZone("fractional", 30),
		)

		Expect(func() {
			// Should panic with "Time.MarshalBinary: zone offset has fractional minute"
			marshalEnvelope(in)
		}).To(Panic())
	})
})

var _ = Describe("func unmarshalEnvelope()", func() {
	var (
		createdAt, scheduledFor time.Time
		in                      *pb.Envelope
	)

	BeforeEach(func() {
		createdAt = time.Now()
		scheduledFor = createdAt.Add(1 * time.Hour)

		createdAtData, err := createdAt.MarshalBinary()
		Expect(err).ShouldNot(HaveOccurred())

		scheduledForData, err := scheduledFor.MarshalBinary()
		Expect(err).ShouldNot(HaveOccurred())

		in = &pb.Envelope{
			MetaData: &pb.MetaData{
				MessageId:     "<id>",
				CausationId:   "<cause>",
				CorrelationId: "<correlation>",
				Source: &pb.Source{
					Application: &pb.Identity{Name: "<app-name>", Key: "<app-key>"},
					Handler:     &pb.Identity{Name: "<handler-name>", Key: "<handler-key>"},
					InstanceId:  "<instance>",
				},
				CreatedAt:    createdAtData,
				ScheduledFor: scheduledForData,
			},
			Packet: &pb.Packet{
				MediaType: MessageA1Packet.MediaType,
				Data:      MessageA1Packet.Data,
			},
		}
	})

	It("unmarshals from protobuf", func() {
		var out envelope.Envelope
		err := unmarshalEnvelope(Marshaler, in, &out)
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
		err := unmarshalEnvelope(Marshaler, in, &out)
		Expect(err).Should(HaveOccurred())
	})

	It("returns an error if the scheduled-for time can not be unmarshaled", func() {
		in.MetaData.ScheduledFor = []byte("not-a-valid-time")

		var out envelope.Envelope
		err := unmarshalEnvelope(Marshaler, in, &out)
		Expect(err).Should(HaveOccurred())
	})

	It("does not return an error if the scheduled-for time is empty", func() {
		in.MetaData.ScheduledFor = nil

		var out envelope.Envelope
		err := unmarshalEnvelope(Marshaler, in, &out)
		Expect(err).ShouldNot(HaveOccurred())
		Expect(out.MetaData.ScheduledFor.IsZero()).To(BeTrue())
	})

	It("returns an error if the source is not valid", func() {
		in.MetaData.Source.Handler = nil

		var out envelope.Envelope
		err := unmarshalEnvelope(Marshaler, in, &out)
		Expect(err).Should(HaveOccurred())
	})

	It("returns an error if the meta-data is not valid", func() {
		in.MetaData.CreatedAt = nil

		var out envelope.Envelope
		err := unmarshalEnvelope(Marshaler, in, &out)
		Expect(err).Should(HaveOccurred())
	})

	It("returns an error if the marshaler fails", func() {
		in.Packet.MediaType = "<unknown>"

		var out envelope.Envelope
		err := unmarshalEnvelope(Marshaler, in, &out)
		Expect(err).Should(HaveOccurred())
	})
})

var _ = Describe("func marshalPacket()", func() {
	It("marshals to protobuf", func() {
		in := marshalkit.Packet{
			MediaType: "<media-type>",
			Data:      []byte("<data>"),
		}

		out := marshalPacket(in)

		Expect(out).To(Equal(&pb.Packet{
			MediaType: "<media-type>",
			Data:      []byte("<data>"),
		}))
	})
})

var _ = Describe("func unmarshalPacket()", func() {
	It("unmarshals from protobuf", func() {
		in := &pb.Packet{
			MediaType: "<media-type>",
			Data:      []byte("<data>"),
		}

		var out marshalkit.Packet
		unmarshalPacket(in, &out)

		Expect(out).To(Equal(marshalkit.Packet{
			MediaType: "<media-type>",
			Data:      []byte("<data>"),
		}))
	})
})
