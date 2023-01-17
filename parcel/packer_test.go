package parcel_test

import (
	"errors"
	"fmt"
	"time"

	. "github.com/dogmatiq/configkit/fixtures"
	"github.com/dogmatiq/configkit/message"
	"github.com/dogmatiq/dogma"
	. "github.com/dogmatiq/dogma/fixtures"
	"github.com/dogmatiq/interopspec/envelopespec"
	"github.com/dogmatiq/marshalkit"
	. "github.com/dogmatiq/marshalkit/fixtures"
	. "github.com/dogmatiq/verity/parcel"
	"github.com/google/uuid"
	. "github.com/jmalloc/gomegax"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/ginkgo/extensions/table"
	. "github.com/onsi/gomega"
)

var _ = Describe("type Packer", func() {
	var (
		seq          int
		now          time.Time
		nowString    string
		app, handler *envelopespec.Identity
		packer       *Packer
	)

	BeforeEach(func() {
		seq = 0

		now = time.Now()
		nowString = marshalkit.MustMarshalEnvelopeTime(now)

		app = &envelopespec.Identity{
			Name: "<app-name>",
			Key:  "<app-key>",
		}

		handler = &envelopespec.Identity{
			Name: "<handler-name>",
			Key:  "<handler-key>",
		}

		packer = &Packer{
			Application: app,
			Marshaler:   Marshaler,
			Produced: message.TypeRoles{
				MessageCType: message.CommandRole,
				MessageEType: message.EventRole,
				MessageTType: message.TimeoutRole,
			},
			Consumed: message.TypeRoles{
				MessageDType: message.CommandRole,
				MessageFType: message.EventRole,
				MessageUType: message.TimeoutRole,
			},
			GenerateID: func() string {
				seq++
				return fmt.Sprintf("%08d", seq)
			},
			Now: func() time.Time {
				return now
			},
		}
	})

	Describe("func PackCommand()", func() {
		It("returns a new envelope", func() {
			p := packer.PackCommand(MessageC1)

			Expect(p).To(EqualX(
				Parcel{
					Envelope: &envelopespec.Envelope{
						MessageId:         "00000001",
						CausationId:       "00000001",
						CorrelationId:     "00000001",
						SourceApplication: app,
						CreatedAt:         nowString,
						Description:       "{C1}",
						PortableName:      MessageCPortableName,
						MediaType:         MessageC1Packet.MediaType,
						Data:              MessageC1Packet.Data,
					},
					Message:   MessageC1,
					CreatedAt: now,
				},
			))
		})

		DescribeTable(
			"it panics if the message type is incorrect",
			func(m dogma.Message, x string) {
				Expect(func() {
					packer.PackCommand(m)
				}).To(PanicWith(x))
			},
			Entry("when the message is unrecognized", MessageX1, "fixtures.MessageX is not a recognised message type"),
			Entry("when the message is an event", MessageE1, "fixtures.MessageE is an event, expected a command"),
			Entry("when the message is a timeout", MessageT1, "fixtures.MessageT is a timeout, expected a command"),
		)

		It("panics if the message is invalid", func() {
			Expect(func() {
				m := MessageC{
					Value: errors.New("<error>"),
				}
				packer.PackCommand(m)
			}).To(PanicWith("fixtures.MessageC command is invalid: <error>"))
		})
	})

	Describe("func PackEvent()", func() {
		It("returns a new envelope", func() {
			p := packer.PackEvent(MessageE1)

			Expect(p).To(EqualX(
				Parcel{
					Envelope: &envelopespec.Envelope{
						MessageId:         "00000001",
						CausationId:       "00000001",
						CorrelationId:     "00000001",
						SourceApplication: app,
						CreatedAt:         nowString,
						Description:       "{E1}",
						PortableName:      MessageEPortableName,
						MediaType:         MessageE1Packet.MediaType,
						Data:              MessageE1Packet.Data,
					},
					Message:   MessageE1,
					CreatedAt: now,
				},
			))
		})

		DescribeTable(
			"it panics if the message type is incorrect",
			func(m dogma.Message, x string) {
				Expect(func() {
					packer.PackEvent(m)
				}).To(PanicWith(x))
			},
			Entry("when the message is unrecognized", MessageX1, "fixtures.MessageX is not a recognised message type"),
			Entry("when the message is a command", MessageC1, "fixtures.MessageC is a command, expected an event"),
			Entry("when the message is a timeout", MessageT1, "fixtures.MessageT is a timeout, expected an event"),
		)

		It("panics if the message is invalid", func() {
			Expect(func() {
				m := MessageE{
					Value: errors.New("<error>"),
				}
				packer.PackEvent(m)
			}).To(PanicWith("fixtures.MessageE event is invalid: <error>"))
		})
	})

	It("generates UUIDs by default", func() {
		packer.GenerateID = nil

		p := packer.PackCommand(MessageC1)

		_, err := uuid.Parse(p.ID())
		Expect(err).ShouldNot(HaveOccurred())
	})

	It("uses the system clock by default", func() {
		packer.Now = nil

		p := packer.PackCommand(MessageC1)

		createdAt, err := marshalkit.UnmarshalEnvelopeTime(p.Envelope.GetCreatedAt())
		Expect(err).ShouldNot(HaveOccurred())
		Expect(createdAt).To(BeTemporally("~", time.Now()))
	})

	Context("child envelopes", func() {
		var parent *envelopespec.Envelope

		BeforeEach(func() {
			parent = &envelopespec.Envelope{
				MessageId:     "<cause>",
				CausationId:   "<cause>",
				CorrelationId: "<cause>",
				SourceApplication: &envelopespec.Identity{
					Name: "<app-name>",
					Key:  "<app-key>",
				},
			}
		})

		Describe("func PackChildCommand()", func() {
			DescribeTable(
				"it returns a new envelope",
				func(cause dogma.Message) {
					p := packer.PackChildCommand(
						Parcel{
							Envelope: parent,
							Message:  cause,
						},
						MessageC1,
						handler,
						"<instance>",
					)

					Expect(p).To(EqualX(
						Parcel{
							Envelope: &envelopespec.Envelope{
								MessageId:         "00000001",
								CausationId:       "<cause>",
								CorrelationId:     "<cause>",
								SourceApplication: app,
								SourceHandler:     handler,
								SourceInstanceId:  "<instance>",
								CreatedAt:         nowString,
								Description:       "{C1}",
								PortableName:      MessageCPortableName,
								MediaType:         MessageC1Packet.MediaType,
								Data:              MessageC1Packet.Data,
							},
							Message:   MessageC1,
							CreatedAt: now,
						},
					))
				},
				Entry("when the cause is an event", MessageF1),
				Entry("when the cause is a timeout", MessageU1),
			)

			DescribeTable(
				"it panics if the cause's message type is incorrect",
				func(cause dogma.Message, x string) {
					Expect(func() {
						packer.PackChildCommand(
							Parcel{
								Envelope: parent,
								Message:  cause,
							},
							MessageC1,
							handler,
							"<instance>",
						)
					}).To(PanicWith(x))
				},
				Entry("when the cause is unrecognized", MessageX1, "fixtures.MessageX is not consumed by this handler"),
				Entry("when the cause is a command", MessageD1, "fixtures.MessageD is a command, expected an event or timeout"),
			)

			DescribeTable(
				"it panics if the message type is incorrect",
				func(m dogma.Message, x string) {
					Expect(func() {
						packer.PackChildCommand(
							Parcel{
								Envelope: parent,
								Message:  MessageF1,
							},
							m,
							handler,
							"<instance>",
						)
					}).To(PanicWith(x))
				},
				Entry("when the message is unrecognized", MessageX1, "fixtures.MessageX is not a recognised message type"),
				Entry("when the message is an event", MessageE1, "fixtures.MessageE is an event, expected a command"),
				Entry("when the message is a timeout", MessageT1, "fixtures.MessageT is a timeout, expected a command"),
			)

			It("panics if the message is invalid", func() {
				Expect(func() {
					packer.PackChildCommand(
						Parcel{
							Envelope: parent,
							Message:  MessageF1,
						},
						MessageC{
							Value: errors.New("<error>"),
						},
						handler,
						"<instance>",
					)
				}).To(PanicWith("fixtures.MessageC command is invalid: <error>"))
			})
		})

		Describe("func PackChildEvent()", func() {
			DescribeTable(
				"it returns a new envelope",
				func(cause dogma.Message) {
					p := packer.PackChildEvent(
						Parcel{
							Envelope: parent,
							Message:  cause,
						},
						MessageE1,
						handler,
						"<instance>",
					)

					Expect(p).To(EqualX(
						Parcel{
							Envelope: &envelopespec.Envelope{
								MessageId:         "00000001",
								CausationId:       "<cause>",
								CorrelationId:     "<cause>",
								SourceApplication: app,
								SourceHandler:     handler,
								SourceInstanceId:  "<instance>",
								CreatedAt:         nowString,
								Description:       "{E1}",
								PortableName:      MessageEPortableName,
								MediaType:         MessageE1Packet.MediaType,
								Data:              MessageE1Packet.Data,
							},
							Message:   MessageE1,
							CreatedAt: now,
						},
					))
				},
				Entry("when the cause is a command", MessageD1),
			)

			DescribeTable(
				"it panics if the cause's message type is incorrect",
				func(cause dogma.Message, x string) {
					Expect(func() {
						packer.PackChildEvent(
							Parcel{
								Envelope: parent,
								Message:  cause,
							},
							MessageE1,
							handler,
							"<instance>",
						)
					}).To(PanicWith(x))
				},
				Entry("when the cause is unrecognized", MessageX1, "fixtures.MessageX is not consumed by this handler"),
				Entry("when the cause is an event", MessageF1, "fixtures.MessageF is an event, expected a command"),
				Entry("when the cause is a timeout", MessageU1, "fixtures.MessageU is a timeout, expected a command"),
			)

			DescribeTable(
				"it panics if the message type is incorrect",
				func(m dogma.Message, x string) {
					Expect(func() {
						packer.PackChildEvent(
							Parcel{
								Envelope: parent,
								Message:  MessageD1,
							},
							m,
							handler,
							"<instance>",
						)
					}).To(PanicWith(x))
				},
				Entry("when the message is unrecognized", MessageX1, "fixtures.MessageX is not a recognised message type"),
				Entry("when the message is a command", MessageC1, "fixtures.MessageC is a command, expected an event"),
				Entry("when the message is a timeout", MessageT1, "fixtures.MessageT is a timeout, expected an event"),
			)

			It("panics if the message is invalid", func() {
				Expect(func() {
					packer.PackChildEvent(
						Parcel{
							Envelope: parent,
							Message:  MessageD1,
						},
						MessageE{
							Value: errors.New("<error>"),
						},
						handler,
						"<instance>",
					)
				}).To(PanicWith("fixtures.MessageE event is invalid: <error>"))
			})
		})

		Describe("func PackChildTimeout()", func() {
			DescribeTable(
				"it returns a new envelope",
				func(cause dogma.Message) {
					scheduledFor := time.Now()
					p := packer.PackChildTimeout(
						Parcel{
							Envelope: parent,
							Message:  cause,
						},
						MessageT1,
						scheduledFor,
						handler,
						"<instance>",
					)

					Expect(p).To(EqualX(
						Parcel{
							Envelope: &envelopespec.Envelope{
								MessageId:         "00000001",
								CausationId:       "<cause>",
								CorrelationId:     "<cause>",
								SourceApplication: app,
								SourceHandler:     handler,
								SourceInstanceId:  "<instance>",
								CreatedAt:         nowString,
								ScheduledFor:      marshalkit.MustMarshalEnvelopeTime(scheduledFor),
								Description:       "{T1}",
								PortableName:      MessageTPortableName,
								MediaType:         MessageT1Packet.MediaType,
								Data:              MessageT1Packet.Data,
							},
							Message:      MessageT1,
							CreatedAt:    now,
							ScheduledFor: scheduledFor,
						},
					))
				},
				Entry("when the cause is an event", MessageF1),
				Entry("when the cause is a timeout", MessageU1),
			)

			DescribeTable(
				"it panics if the cause's message type is incorrect",
				func(cause dogma.Message, x string) {
					Expect(func() {
						packer.PackChildTimeout(
							Parcel{
								Envelope: parent,
								Message:  cause,
							},
							MessageT1,
							time.Now(),
							handler,
							"<instance>",
						)
					}).To(PanicWith(x))
				},
				Entry("when the cause is unrecognized", MessageX1, "fixtures.MessageX is not consumed by this handler"),
				Entry("when the cause is a command", MessageD1, "fixtures.MessageD is a command, expected an event or timeout"),
			)

			DescribeTable(
				"it panics if the message type is incorrect",
				func(m dogma.Message, x string) {
					Expect(func() {
						packer.PackChildTimeout(
							Parcel{
								Envelope: parent,
								Message:  MessageF1,
							},
							m,
							time.Now(),
							handler,
							"<instance>",
						)
					}).To(PanicWith(x))
				},
				Entry("when the message is unrecognized", MessageX1, "fixtures.MessageX is not a recognised message type"),
				Entry("when the message is a command", MessageC1, "fixtures.MessageC is a command, expected a timeout"),
				Entry("when the message is an event", MessageE1, "fixtures.MessageE is an event, expected a timeout"),
			)
		})

		It("panics if the message is invalid", func() {
			Expect(func() {
				packer.PackChildTimeout(
					Parcel{
						Envelope: parent,
						Message:  MessageF1,
					},
					MessageT{
						Value: errors.New("<error>"),
					},
					time.Now(),
					handler,
					"<instance>",
				)
			}).To(PanicWith("fixtures.MessageT timeout is invalid: <error>"))
		})
	})
})
