package parcel_test

import (
	"fmt"
	"time"

	"github.com/dogmatiq/configkit/message"
	"github.com/dogmatiq/dogma"
	. "github.com/dogmatiq/enginekit/enginetest/stubs"
	"github.com/dogmatiq/interopspec/envelopespec"
	"github.com/dogmatiq/marshalkit"
	. "github.com/dogmatiq/marshalkit/fixtures"
	. "github.com/dogmatiq/verity/fixtures"
	. "github.com/dogmatiq/verity/parcel"
	"github.com/google/uuid"
	. "github.com/jmalloc/gomegax"
	. "github.com/onsi/ginkgo/v2"
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
			Key:  DefaultAppKey,
		}

		handler = &envelopespec.Identity{
			Name: "<handler-name>",
			Key:  DefaultHandlerKey,
		}

		packer = &Packer{
			Application: app,
			Marshaler:   Marshaler,
			Produced: message.TypeRoles{
				message.TypeFor[CommandStub[TypeC]](): message.CommandRole,
				message.TypeFor[EventStub[TypeE]]():   message.EventRole,
				message.TypeFor[TimeoutStub[TypeT]](): message.TimeoutRole,
			},
			Consumed: message.TypeRoles{
				message.TypeFor[CommandStub[TypeD]](): message.CommandRole,
				message.TypeFor[EventStub[TypeF]]():   message.EventRole,
				message.TypeFor[TimeoutStub[TypeU]](): message.TimeoutRole,
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
			p := packer.PackCommand(CommandC1)

			Expect(p).To(EqualX(
				Parcel{
					Envelope: &envelopespec.Envelope{
						MessageId:         "00000001",
						CausationId:       "00000001",
						CorrelationId:     "00000001",
						SourceApplication: app,
						CreatedAt:         nowString,
						Description:       "command(stubs.TypeC:C1, valid)",
						PortableName:      "CommandStub[TypeC]",
						MediaType:         `application/json; type="CommandStub[TypeC]"`,
						Data:              []byte(`{"content":"C1"}`),
					},
					Message:   CommandC1,
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
			Entry("when the message is unrecognized", CommandX1, "stubs.CommandStub[TypeX] is not a recognised message type"),
			Entry("when the message is an event", EventE1, "stubs.EventStub[TypeE] is an event, expected a command"),
			Entry("when the message is a timeout", TimeoutT1, "stubs.TimeoutStub[TypeT] is a timeout, expected a command"),
		)

		It("panics if the message is invalid", func() {
			Expect(func() {
				m := CommandStub[TypeC]{
					ValidationError: "<error>",
				}
				packer.PackCommand(m)
			}).To(PanicWith("stubs.CommandStub[github.com/dogmatiq/enginekit/enginetest/stubs.TypeC] command is invalid: <error>"))
		})
	})

	Describe("func PackEvent()", func() {
		It("returns a new envelope", func() {
			p := packer.PackEvent(EventE1)

			Expect(p).To(EqualX(
				Parcel{
					Envelope: &envelopespec.Envelope{
						MessageId:         "00000001",
						CausationId:       "00000001",
						CorrelationId:     "00000001",
						SourceApplication: app,
						CreatedAt:         nowString,
						Description:       "event(stubs.TypeE:E1, valid)",
						PortableName:      "EventStub[TypeE]",
						MediaType:         `application/json; type="EventStub[TypeE]"`,
						Data:              []byte(`{"content":"E1"}`),
					},
					Message:   EventE1,
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
			Entry("when the message is unrecognized", EventX1, "stubs.EventStub[TypeX] is not a recognised message type"),
			Entry("when the message is a command", CommandC1, "stubs.CommandStub[TypeC] is a command, expected an event"),
			Entry("when the message is a timeout", TimeoutT1, "stubs.TimeoutStub[TypeT] is a timeout, expected an event"),
		)

		It("panics if the message is invalid", func() {
			Expect(func() {
				m := EventStub[TypeE]{
					ValidationError: "<error>",
				}
				packer.PackEvent(m)
			}).To(PanicWith("stubs.EventStub[github.com/dogmatiq/enginekit/enginetest/stubs.TypeE] event is invalid: <error>"))
		})
	})

	It("generates UUIDs by default", func() {
		packer.GenerateID = nil

		p := packer.PackCommand(CommandC1)

		_, err := uuid.Parse(p.ID())
		Expect(err).ShouldNot(HaveOccurred())
	})

	It("uses the system clock by default", func() {
		packer.Now = nil

		p := packer.PackCommand(CommandC1)

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
					Key:  DefaultAppKey,
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
						CommandC1,
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
								Description:       "command(stubs.TypeC:C1, valid)",
								PortableName:      "CommandStub[TypeC]",
								MediaType:         `application/json; type="CommandStub[TypeC]"`,
								Data:              []byte(`{"content":"C1"}`),
							},
							Message:   CommandC1,
							CreatedAt: now,
						},
					))
				},
				Entry("when the cause is an event", EventF1),
				Entry("when the cause is a timeout", TimeoutU1),
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
							CommandC1,
							handler,
							"<instance>",
						)
					}).To(PanicWith(x))
				},
				Entry("when the cause is unrecognized", EventX1, "stubs.EventStub[TypeX] is not consumed by this handler"),
				Entry("when the cause is a command", CommandD1, "stubs.CommandStub[TypeD] is a command, expected an event or timeout"),
			)

			DescribeTable(
				"it panics if the message type is incorrect",
				func(m dogma.Message, x string) {
					Expect(func() {
						packer.PackChildCommand(
							Parcel{
								Envelope: parent,
								Message:  EventF1,
							},
							m,
							handler,
							"<instance>",
						)
					}).To(PanicWith(x))
				},
				Entry("when the message is unrecognized", CommandX1, "stubs.CommandStub[TypeX] is not a recognised message type"),
				Entry("when the message is an event", EventE1, "stubs.EventStub[TypeE] is an event, expected a command"),
				Entry("when the message is a timeout", TimeoutT1, "stubs.TimeoutStub[TypeT] is a timeout, expected a command"),
			)

			It("panics if the message is invalid", func() {
				Expect(func() {
					packer.PackChildCommand(
						Parcel{
							Envelope: parent,
							Message:  EventF1,
						},
						CommandStub[TypeC]{
							ValidationError: "<error>",
						},
						handler,
						"<instance>",
					)
				}).To(PanicWith("stubs.CommandStub[github.com/dogmatiq/enginekit/enginetest/stubs.TypeC] command is invalid: <error>"))
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
						EventE1,
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
								Description:       "event(stubs.TypeE:E1, valid)",
								PortableName:      "EventStub[TypeE]",
								MediaType:         `application/json; type="EventStub[TypeE]"`,
								Data:              []byte(`{"content":"E1"}`),
							},
							Message:   EventE1,
							CreatedAt: now,
						},
					))
				},
				Entry("when the cause is a command", CommandD1),
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
							EventE1,
							handler,
							"<instance>",
						)
					}).To(PanicWith(x))
				},
				Entry("when the cause is unrecognized", CommandX1, "stubs.CommandStub[TypeX] is not consumed by this handler"),
				Entry("when the cause is an event", EventF1, "stubs.EventStub[TypeF] is an event, expected a command"),
				Entry("when the cause is a timeout", TimeoutU1, "stubs.TimeoutStub[TypeU] is a timeout, expected a command"),
			)

			DescribeTable(
				"it panics if the message type is incorrect",
				func(m dogma.Message, x string) {
					Expect(func() {
						packer.PackChildEvent(
							Parcel{
								Envelope: parent,
								Message:  CommandD1,
							},
							m,
							handler,
							"<instance>",
						)
					}).To(PanicWith(x))
				},
				Entry("when the message is unrecognized", EventX1, "stubs.EventStub[TypeX] is not a recognised message type"),
				Entry("when the message is a command", CommandC1, "stubs.CommandStub[TypeC] is a command, expected an event"),
				Entry("when the message is a timeout", TimeoutT1, "stubs.TimeoutStub[TypeT] is a timeout, expected an event"),
			)

			It("panics if the message is invalid", func() {
				Expect(func() {
					packer.PackChildEvent(
						Parcel{
							Envelope: parent,
							Message:  CommandD1,
						},
						EventStub[TypeE]{
							ValidationError: "<error>",
						},
						handler,
						"<instance>",
					)
				}).To(PanicWith("stubs.EventStub[github.com/dogmatiq/enginekit/enginetest/stubs.TypeE] event is invalid: <error>"))
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
						TimeoutT1,
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
								Description:       "timeout(stubs.TypeT:T1, valid)",
								PortableName:      "TimeoutStub[TypeT]",
								MediaType:         `application/json; type="TimeoutStub[TypeT]"`,
								Data:              []byte(`{"content":"T1"}`),
							},
							Message:      TimeoutT1,
							CreatedAt:    now,
							ScheduledFor: scheduledFor,
						},
					))
				},
				Entry("when the cause is an event", EventF1),
				Entry("when the cause is a timeout", TimeoutU1),
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
							TimeoutT1,
							time.Now(),
							handler,
							"<instance>",
						)
					}).To(PanicWith(x))
				},
				Entry("when the cause is unrecognized", EventX1, "stubs.EventStub[TypeX] is not consumed by this handler"),
				Entry("when the cause is a command", CommandD1, "stubs.CommandStub[TypeD] is a command, expected an event or timeout"),
			)

			DescribeTable(
				"it panics if the message type is incorrect",
				func(m dogma.Message, x string) {
					Expect(func() {
						packer.PackChildTimeout(
							Parcel{
								Envelope: parent,
								Message:  EventF1,
							},
							m,
							time.Now(),
							handler,
							"<instance>",
						)
					}).To(PanicWith(x))
				},
				Entry("when the message is unrecognized", TimeoutX1, "stubs.TimeoutStub[TypeX] is not a recognised message type"),
				Entry("when the message is a command", CommandC1, "stubs.CommandStub[TypeC] is a command, expected a timeout"),
				Entry("when the message is an event", EventE1, "stubs.EventStub[TypeE] is an event, expected a timeout"),
			)
		})

		It("panics if the message is invalid", func() {
			Expect(func() {
				packer.PackChildTimeout(
					Parcel{
						Envelope: parent,
						Message:  EventF1,
					},
					TimeoutStub[TypeT]{
						ValidationError: "<error>",
					},
					time.Now(),
					handler,
					"<instance>",
				)
			}).To(PanicWith("stubs.TimeoutStub[github.com/dogmatiq/enginekit/enginetest/stubs.TypeT] timeout is invalid: <error>"))
		})
	})
})
