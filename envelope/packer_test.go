package envelope_test

import (
	"fmt"
	"time"

	"github.com/dogmatiq/configkit"
	. "github.com/dogmatiq/configkit/fixtures"
	"github.com/dogmatiq/configkit/message"
	"github.com/dogmatiq/dogma"
	. "github.com/dogmatiq/dogma/fixtures"
	"github.com/dogmatiq/infix/draftspecs/envelopespec"
	. "github.com/dogmatiq/infix/envelope"
	. "github.com/dogmatiq/infix/internal/x/gomegax"
	"github.com/google/uuid"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/ginkgo/extensions/table"
	. "github.com/onsi/gomega"
)

var _ = Describe("type Packer", func() {
	var (
		seq    int
		now    time.Time
		packer *Packer
	)

	BeforeEach(func() {
		seq = 0
		now = time.Now()
		packer = &Packer{
			Application: configkit.MustNewIdentity("<app-name>", "<app-key>"),
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
			env := packer.PackCommand(MessageC1)

			Expect(env).To(Equal(
				&Envelope{
					MetaData: MetaData{
						MessageID:     "00000001",
						CausationID:   "00000001",
						CorrelationID: "00000001",
						Source: Source{
							Application: configkit.MustNewIdentity("<app-name>", "<app-key>"),
						},
						CreatedAt: now,
					},
					Message: MessageC1,
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
	})

	Describe("func PackEvent()", func() {
		It("returns a new envelope", func() {
			env := packer.PackEvent(MessageE1)

			Expect(env).To(Equal(
				&Envelope{
					MetaData: MetaData{
						MessageID:     "00000001",
						CausationID:   "00000001",
						CorrelationID: "00000001",
						Source: Source{
							Application: configkit.MustNewIdentity("<app-name>", "<app-key>"),
						},
						CreatedAt: now,
					},
					Message: MessageE1,
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
	})

	It("generates UUIDs by default", func() {
		packer.GenerateID = nil

		env := packer.PackCommand(MessageC1)
		_, err := uuid.Parse(env.MessageID)
		Expect(err).ShouldNot(HaveOccurred())
	})

	It("uses the system clock by default", func() {
		packer.Now = nil

		env := packer.PackCommand(MessageC1)
		Expect(env.CreatedAt).To(BeTemporally("~", time.Now()))
	})

	Context("child envelopes", func() {
		var parent *envelopespec.Envelope

		BeforeEach(func() {
			parent = &envelopespec.Envelope{
				MetaData: &envelopespec.MetaData{
					MessageId:     "<cause>",
					CausationId:   "<cause>",
					CorrelationId: "<cause>",
					Source: &envelopespec.Source{
						Application: &envelopespec.Identity{
							Name: "<app-name>",
							Key:  "<app-key>",
						},
					},
				},
			}
		})

		Describe("func PackChildCommand()", func() {
			DescribeTable(
				"it returns a new envelope",
				func(cause dogma.Message) {
					env := packer.PackChildCommand(
						parent,
						cause,
						MessageC1,
						configkit.MustNewIdentity("<handler-name>", "<handler-key>"),
						"<instance>",
					)

					Expect(env).To(Equal(
						&Envelope{
							MetaData: MetaData{
								MessageID:     "00000001",
								CausationID:   "<cause>",
								CorrelationID: "<cause>",
								Source: Source{
									Application: configkit.MustNewIdentity("<app-name>", "<app-key>"),
									Handler:     configkit.MustNewIdentity("<handler-name>", "<handler-key>"),
									InstanceID:  "<instance>",
								},
								CreatedAt: now,
							},
							Message: MessageC1,
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
							parent,
							cause,
							MessageC1,
							configkit.MustNewIdentity("<handler-name>", "<handler-key>"),
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
							parent,
							MessageF1,
							m,
							configkit.MustNewIdentity("<handler-name>", "<handler-key>"),
							"<instance>",
						)
					}).To(PanicWith(x))
				},
				Entry("when the message is unrecognized", MessageX1, "fixtures.MessageX is not a recognised message type"),
				Entry("when the message is an event", MessageE1, "fixtures.MessageE is an event, expected a command"),
				Entry("when the message is a timeout", MessageT1, "fixtures.MessageT is a timeout, expected a command"),
			)
		})

		Describe("func PackChildEvent()", func() {
			DescribeTable(
				"it returns a new envelope",
				func(cause dogma.Message) {
					env := packer.PackChildEvent(
						parent,
						cause,
						MessageE1,
						configkit.MustNewIdentity("<handler-name>", "<handler-key>"),
						"<instance>",
					)

					Expect(env).To(Equal(
						&Envelope{
							MetaData: MetaData{
								MessageID:     "00000001",
								CausationID:   "<cause>",
								CorrelationID: "<cause>",
								Source: Source{
									Application: configkit.MustNewIdentity("<app-name>", "<app-key>"),
									Handler:     configkit.MustNewIdentity("<handler-name>", "<handler-key>"),
									InstanceID:  "<instance>",
								},
								CreatedAt: now,
							},
							Message: MessageE1,
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
							parent,
							cause,
							MessageE1,
							configkit.MustNewIdentity("<handler-name>", "<handler-key>"),
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
							parent,
							MessageD1,
							m,
							configkit.MustNewIdentity("<handler-name>", "<handler-key>"),
							"<instance>",
						)
					}).To(PanicWith(x))
				},
				Entry("when the message is unrecognized", MessageX1, "fixtures.MessageX is not a recognised message type"),
				Entry("when the message is a command", MessageC1, "fixtures.MessageC is a command, expected an event"),
				Entry("when the message is a timeout", MessageT1, "fixtures.MessageT is a timeout, expected an event"),
			)
		})

		Describe("func PackChildTimeout()", func() {
			DescribeTable(
				"it returns a new envelope",
				func(cause dogma.Message) {
					scheduledFor := time.Now()
					env := packer.PackChildTimeout(
						parent,
						cause,
						MessageT1,
						scheduledFor,
						configkit.MustNewIdentity("<handler-name>", "<handler-key>"),
						"<instance>",
					)

					Expect(env).To(Equal(
						&Envelope{
							MetaData: MetaData{
								MessageID:     "00000001",
								CausationID:   "<cause>",
								CorrelationID: "<cause>",
								Source: Source{
									Application: configkit.MustNewIdentity("<app-name>", "<app-key>"),
									Handler:     configkit.MustNewIdentity("<handler-name>", "<handler-key>"),
									InstanceID:  "<instance>",
								},
								CreatedAt:    now,
								ScheduledFor: scheduledFor,
							},
							Message: MessageT1,
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
							parent,
							cause,
							MessageT1,
							time.Now(),
							configkit.MustNewIdentity("<handler-name>", "<handler-key>"),
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
							parent,
							MessageF1,
							m,
							time.Now(),
							configkit.MustNewIdentity("<handler-name>", "<handler-key>"),
							"<instance>",
						)
					}).To(PanicWith(x))
				},
				Entry("when the message is unrecognized", MessageX1, "fixtures.MessageX is not a recognised message type"),
				Entry("when the message is a command", MessageC1, "fixtures.MessageC is a command, expected a timeout"),
				Entry("when the message is an event", MessageE1, "fixtures.MessageE is an event, expected a timeout"),
			)
		})
	})
})
