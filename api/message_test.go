package api

import (
	"time"

	"github.com/dogmatiq/configkit"
	. "github.com/dogmatiq/dogma/fixtures"
	"github.com/dogmatiq/infix/api/internal/pb"
	imessage "github.com/dogmatiq/infix/message"
	. "github.com/dogmatiq/marshalkit/fixtures"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Context("message envelopes", func() {
	var (
		createdAt    = time.Date(2001, 2, 3, 4, 5, 6, 7, time.UTC)
		scheduledFor = time.Date(2011, 12, 13, 14, 15, 16, 17, time.UTC)
	)
	Describe("func MarshalMessageEnvelope()", func() {
		var src *imessage.Envelope

		BeforeEach(func() {
			src = &imessage.Envelope{
				MetaData: imessage.MetaData{
					MessageID:     "<id>",
					CausationID:   "<cause>",
					CorrelationID: "<correlation>",
					Source: imessage.Source{
						App:        configkit.MustNewIdentity("<app-name>", "<app-key>"),
						Handler:    configkit.MustNewIdentity("<handler-name>", "<handler-key>"),
						InstanceID: "<instance>",
					},
					CreatedAt:    createdAt,
					ScheduledFor: scheduledFor,
				},
				Message: MessageA1,
				Packet:  MessageA1Packet,
			}
		})

		It("marshals to protobuf", func() {
			dest := marshalMessageEnvelope(src)

			Expect(dest).To(Equal(&pb.MessageEnvelope{
				MetaData: &pb.MessageMetaData{
					MessageId:     "<id>",
					CausationId:   "<cause>",
					CorrelationId: "<correlation>",
					Source: &pb.MessageSource{
						App:        &pb.Identity{Name: "<app-name>", Key: "<app-key>"},
						Handler:    &pb.Identity{Name: "<handler-name>", Key: "<handler-key>"},
						InstanceId: "<instance>",
					},
					CreatedAt:    "2001-02-03T04:05:06.000000007Z",
					ScheduledFor: "2011-12-13T14:15:16.000000017Z",
				},
				Packet: &pb.Packet{
					MediaType: MessageA1Packet.MediaType,
					Data:      MessageA1Packet.Data,
				},
			}))
		})
	})

	Describe("func unmarshalMessageEnvelope()", func() {
		var src *pb.MessageEnvelope

		BeforeEach(func() {
			src = &pb.MessageEnvelope{
				MetaData: &pb.MessageMetaData{
					MessageId:     "<id>",
					CausationId:   "<cause>",
					CorrelationId: "<correlation>",
					Source: &pb.MessageSource{
						App:        &pb.Identity{Name: "<app-name>", Key: "<app-key>"},
						Handler:    &pb.Identity{Name: "<handler-name>", Key: "<handler-key>"},
						InstanceId: "<instance>",
					},
					CreatedAt:    "2001-02-03T04:05:06.000000007Z",
					ScheduledFor: "2011-12-13T14:15:16.000000017Z",
				},
				Packet: &pb.Packet{
					MediaType: MessageA1Packet.MediaType,
					Data:      MessageA1Packet.Data,
				},
			}
		})

		It("unmarshals from protobuf", func() {
			var dest imessage.Envelope
			err := unmarshalMessageEnvelope(Marshaler, src, &dest)
			Expect(err).ShouldNot(HaveOccurred())

			Expect(dest.CreatedAt).To(BeTemporally("==", createdAt))
			Expect(dest.ScheduledFor).To(BeTemporally("==", scheduledFor))

			// clear values for comparison below
			dest.CreatedAt = time.Time{}
			dest.ScheduledFor = time.Time{}

			Expect(dest).To(Equal(imessage.Envelope{
				MetaData: imessage.MetaData{
					MessageID:     "<id>",
					CausationID:   "<cause>",
					CorrelationID: "<correlation>",
					Source: imessage.Source{
						App:        configkit.MustNewIdentity("<app-name>", "<app-key>"),
						Handler:    configkit.MustNewIdentity("<handler-name>", "<handler-key>"),
						InstanceID: "<instance>",
					},
				},
				Message: MessageA1,
				Packet:  MessageA1Packet,
			}))
		})

		It("returns an error if the created-at time can not be unmarshaled", func() {
			src.MetaData.CreatedAt = "not-a-valid-time"

			var dest imessage.Envelope
			err := unmarshalMessageEnvelope(Marshaler, src, &dest)
			Expect(err).Should(HaveOccurred())
		})

		It("returns an error if the scheduled-for time can not be unmarshaled", func() {
			src.MetaData.ScheduledFor = "not-a-valid-time"

			var dest imessage.Envelope
			err := unmarshalMessageEnvelope(Marshaler, src, &dest)
			Expect(err).Should(HaveOccurred())
		})

		It("returns an error if the source app is invalid", func() {
			src.MetaData.Source.App = nil

			var dest imessage.Envelope
			err := unmarshalMessageEnvelope(Marshaler, src, &dest)
			Expect(err).Should(HaveOccurred())
		})

		It("returns an error if the source handler is invalid", func() {
			src.MetaData.Source.Handler = &pb.Identity{Name: "", Key: "<key>"}

			var dest imessage.Envelope
			err := unmarshalMessageEnvelope(Marshaler, src, &dest)
			Expect(err).Should(HaveOccurred())
		})

		It("does not return an error if the source handler and instance are empty", func() {
			src.MetaData.Source.Handler = nil
			src.MetaData.Source.InstanceId = ""

			var dest imessage.Envelope
			err := unmarshalMessageEnvelope(Marshaler, src, &dest)
			Expect(err).ShouldNot(HaveOccurred())
		})

		It("returns an error if the meta-data is not valid", func() {
			src.MetaData.CreatedAt = ""

			var dest imessage.Envelope
			err := unmarshalMessageEnvelope(Marshaler, src, &dest)
			Expect(err).Should(HaveOccurred())
		})

		It("returns an error if the marshaler fails", func() {
			src.Packet.MediaType = "<unknown>"

			var dest imessage.Envelope
			err := unmarshalMessageEnvelope(Marshaler, src, &dest)
			Expect(err).Should(HaveOccurred())
		})
	})
})
