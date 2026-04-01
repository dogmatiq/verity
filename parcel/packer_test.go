package parcel_test

import (
	"time"

	"github.com/dogmatiq/dogma"
	"github.com/dogmatiq/enginekit/collections/sets"
	. "github.com/dogmatiq/enginekit/enginetest/stubs"
	"github.com/dogmatiq/enginekit/enginetest/uuidtest"
	"github.com/dogmatiq/enginekit/message"
	"github.com/dogmatiq/enginekit/protobuf/envelopepb"
	"github.com/dogmatiq/enginekit/protobuf/identitypb"
	"github.com/dogmatiq/enginekit/protobuf/uuidpb"
	. "github.com/dogmatiq/verity/fixtures"
	. "github.com/dogmatiq/verity/parcel"
	"github.com/google/uuid"
	. "github.com/jmalloc/gomegax"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var _ = Describe("type Packer", func() {
	var (
		seq          *uuidtest.Sequence
		now          time.Time
		app, handler *identitypb.Identity
		packer       *Packer
	)

	BeforeEach(func() {
		seq = uuidtest.NewSequence()

		now = time.Now()

		app = identitypb.MustParse("<app-name>", DefaultAppKey)

		handler = identitypb.MustParse("<handler-name>", DefaultHandlerKey)

		packer = &Packer{
			Application: app,
			Produced: sets.New(
				message.TypeFor[*CommandStub[TypeC]](),
				message.TypeFor[*EventStub[TypeE]](),
				message.TypeFor[*TimeoutStub[TypeT]](),
			),
			Consumed: sets.New(
				message.TypeFor[*CommandStub[TypeD]](),
				message.TypeFor[*EventStub[TypeF]](),
				message.TypeFor[*TimeoutStub[TypeU]](),
			),
			GenerateID: seq.Next,
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
					Envelope: &envelopepb.Envelope{
						MessageId:         seq.Nth(0),
						CausationId:       seq.Nth(0),
						CorrelationId:     seq.Nth(0),
						SourceApplication: app,
						CreatedAt:         timestamppb.New(now),
						Description:       "command(stubs.TypeC:C1, valid)",
						TypeId:            MessageTypeUUID[*CommandStub[TypeC]](),
						Data:              []byte(`{"content":"C1"}`),
					},
					Message:   CommandC1,
					CreatedAt: now,
				},
			))
		})

		It("panics if the message is not recognized", func() {
			Expect(func() {
				packer.PackCommand(CommandX1)
			}).To(PanicWith("stubs.CommandStub[TypeX] is not a recognized message type"))
		})

		It("panics if the message is invalid", func() {
			Expect(func() {
				m := &CommandStub[TypeC]{
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
					Envelope: &envelopepb.Envelope{
						MessageId:         seq.Nth(0),
						CausationId:       seq.Nth(0),
						CorrelationId:     seq.Nth(0),
						SourceApplication: app,
						CreatedAt:         timestamppb.New(now),
						TypeId:            MessageTypeUUID[*EventStub[TypeE]](),
						Data:              []byte(`{"content":"E1"}`),
					},
					Message:   EventE1,
					CreatedAt: now,
				},
			))
		})

		It("panics if the message is not recognized", func() {
			Expect(func() {
				packer.PackEvent(EventX1)
			}).To(PanicWith("stubs.EventStub[TypeX] is not a recognized message type"))
		})

		It("panics if the message is invalid", func() {
			Expect(func() {
				m := &EventStub[TypeE]{
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

		createdAt := p.Envelope.GetCreatedAt().AsTime()
		Expect(createdAt).To(BeTemporally("~", time.Now()))
	})

	Context("child envelopes", func() {
		var parent *envelopepb.Envelope

		BeforeEach(func() {
			causeID := uuidpb.Generate()
			parent = &envelopepb.Envelope{
				MessageId:     causeID,
				CausationId:   causeID,
				CorrelationId: causeID,
				SourceApplication: identitypb.New(
					"<app-name>",
					uuidpb.MustParse(DefaultAppKey),
				),
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
							Envelope: &envelopepb.Envelope{
								MessageId:         seq.Nth(0),
								CausationId:       parent.GetMessageId(),
								CorrelationId:     parent.GetMessageId(),
								SourceApplication: app,
								SourceHandler:     handler,
								SourceInstanceId:  "<instance>",
								CreatedAt:         timestamppb.New(now),
								Description:       "command(stubs.TypeC:C1, valid)",
								TypeId:            MessageTypeUUID[*CommandStub[TypeC]](),
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

			It("panics if the causal message is not recognized", func() {
				Expect(func() {
					packer.PackChildCommand(
						Parcel{
							Envelope: parent,
							Message:  EventX1,
						},
						CommandC1,
						handler,
						"<instance>",
					)
				}).To(PanicWith("stubs.EventStub[TypeX] is not consumed by this handler"))
			})

			It("panics if the message is not recognized", func() {
				Expect(func() {
					packer.PackChildCommand(
						Parcel{
							Envelope: parent,
							Message:  EventF1,
						},
						CommandX1,
						handler,
						"<instance>",
					)
				}).To(PanicWith("stubs.CommandStub[TypeX] is not a recognized message type"))
			})

			It("panics if the message is invalid", func() {
				Expect(func() {
					packer.PackChildCommand(
						Parcel{
							Envelope: parent,
							Message:  EventF1,
						},
						&CommandStub[TypeC]{
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
							Envelope: &envelopepb.Envelope{
								MessageId:         seq.Nth(0),
								CausationId:       parent.GetMessageId(),
								CorrelationId:     parent.GetMessageId(),
								SourceApplication: app,
								SourceHandler:     handler,
								SourceInstanceId:  "<instance>",
								CreatedAt:         timestamppb.New(now),
								Description:       "event(stubs.TypeE:E1, valid)",
								TypeId:            MessageTypeUUID[*EventStub[TypeE]](),
								Data:              []byte(`{"content":"E1"}`),
							},
							Message:   EventE1,
							CreatedAt: now,
						},
					))
				},
				Entry("when the cause is a command", CommandD1),
			)

			It("panics if the causal message is not recognized", func() {
				Expect(func() {
					packer.PackChildEvent(
						Parcel{
							Envelope: parent,
							Message:  CommandX1,
						},
						EventE1,
						handler,
						"<instance>",
					)
				}).To(PanicWith("stubs.CommandStub[TypeX] is not consumed by this handler"))
			})

			It("panics if the message is not recognized", func() {
				Expect(func() {
					packer.PackChildEvent(
						Parcel{
							Envelope: parent,
							Message:  EventF1,
						},
						EventX1,
						handler,
						"<instance>",
					)
				}).To(PanicWith("stubs.EventStub[TypeX] is not a recognized message type"))
			})

			It("panics if the message is invalid", func() {
				Expect(func() {
					packer.PackChildEvent(
						Parcel{
							Envelope: parent,
							Message:  CommandD1,
						},
						&EventStub[TypeE]{
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
							Envelope: &envelopepb.Envelope{
								MessageId:         uuidpb.Generate(), // TODO,
								CausationId:       uuidpb.Generate(), // TODO,
								CorrelationId:     uuidpb.Generate(), // TODO,
								SourceApplication: app,
								SourceHandler:     handler,
								SourceInstanceId:  "<instance>",
								CreatedAt:         timestamppb.New(now),
								ScheduledFor:      timestamppb.New(scheduledFor),
								Description:       "timeout(stubs.TypeT:T1, valid)",
								TypeId:            MessageTypeUUID[*TimeoutStub[TypeT]](),
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

			It("panics if the causal message is not recognized", func() {
				Expect(func() {
					packer.PackChildTimeout(
						Parcel{
							Envelope: parent,
							Message:  EventX1,
						},
						TimeoutX1,
						time.Now(),
						handler,
						"<instance>",
					)
				}).To(PanicWith("stubs.EventStub[TypeX] is not consumed by this handler"))
			})

			It("panics if the message is not recognized", func() {
				Expect(func() {
					packer.PackChildTimeout(
						Parcel{
							Envelope: parent,
							Message:  EventF1,
						},
						TimeoutX1,
						time.Now(),
						handler,
						"<instance>",
					)
				}).To(PanicWith("stubs.TimeoutStub[TypeX] is not a recognized message type"))
			})
		})

		It("panics if the message is invalid", func() {
			Expect(func() {
				packer.PackChildTimeout(
					Parcel{
						Envelope: parent,
						Message:  EventF1,
					},
					&TimeoutStub[TypeT]{
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
