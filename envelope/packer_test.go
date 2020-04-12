package envelope_test

import (
	"fmt"
	"time"

	"github.com/dogmatiq/configkit"
	. "github.com/dogmatiq/configkit/fixtures"
	"github.com/dogmatiq/configkit/message"
	"github.com/dogmatiq/dogma"
	. "github.com/dogmatiq/dogma/fixtures"
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

	Describe("func PackChildCommand()", func() {
		var parent *Envelope

		BeforeEach(func() {
			parent = &Envelope{
				MetaData: MetaData{
					MessageID:     "<parent>",
					CausationID:   "<parent>",
					CorrelationID: "<parent>",
					Source: Source{
						Application: configkit.MustNewIdentity("<app-name>", "<app-key>"),
					},
				},
			}
		})

		DescribeTable(
			"it returns a new envelope",
			func(p dogma.Message) {
				parent.Message = p

				env := packer.PackChildCommand(
					parent,
					MessageC1,
					configkit.MustNewIdentity("<handler-name>", "<handler-key>"),
					"<instance>",
				)

				Expect(env).To(Equal(
					&Envelope{
						MetaData: MetaData{
							MessageID:     "00000001",
							CausationID:   "<parent>",
							CorrelationID: "<parent>",
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
			Entry("when the parent is an event", MessageF1),
			Entry("when the parent is a timeout", MessageU1),
		)

		DescribeTable(
			"it panics if the parent message type is incorrect",
			func(p dogma.Message, x string) {
				parent.Message = p

				Expect(func() {
					packer.PackChildCommand(
						parent,
						MessageC1,
						configkit.MustNewIdentity("<handler-name>", "<handler-key>"),
						"<instance>",
					)
				}).To(PanicWith(x))
			},
			Entry("when the message is unrecognized", MessageX1, "fixtures.MessageX is not consumed by this handler"),
			Entry("when the message is a command", MessageD1, "fixtures.MessageD is a command, expected an event or timeout"),
		)

		DescribeTable(
			"it panics if the message type is incorrect",
			func(m dogma.Message, x string) {
				parent.Message = MessageF1

				Expect(func() {
					packer.PackChildCommand(
						parent,
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
		var parent *Envelope

		BeforeEach(func() {
			parent = &Envelope{
				MetaData: MetaData{
					MessageID:     "<parent>",
					CausationID:   "<parent>",
					CorrelationID: "<parent>",
					Source: Source{
						Application: configkit.MustNewIdentity("<app-name>", "<app-key>"),
					},
				},
			}
		})

		DescribeTable(
			"it returns a new envelope",
			func(p dogma.Message) {
				parent.Message = p

				env := packer.PackChildEvent(
					parent,
					MessageE1,
					configkit.MustNewIdentity("<handler-name>", "<handler-key>"),
					"<instance>",
				)

				Expect(env).To(Equal(
					&Envelope{
						MetaData: MetaData{
							MessageID:     "00000001",
							CausationID:   "<parent>",
							CorrelationID: "<parent>",
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
			Entry("when the parent is a command", MessageD1),
		)

		DescribeTable(
			"it panics if the parent message type is incorrect",
			func(p dogma.Message, x string) {
				parent.Message = p

				Expect(func() {
					packer.PackChildEvent(
						parent,
						MessageE1,
						configkit.MustNewIdentity("<handler-name>", "<handler-key>"),
						"<instance>",
					)
				}).To(PanicWith(x))
			},
			Entry("when the message is unrecognized", MessageX1, "fixtures.MessageX is not consumed by this handler"),
			Entry("when the message is an event", MessageF1, "fixtures.MessageF is an event, expected a command"),
			Entry("when the message is a timeout", MessageU1, "fixtures.MessageU is a timeout, expected a command"),
		)

		DescribeTable(
			"it panics if the message type is incorrect",
			func(m dogma.Message, x string) {
				parent.Message = MessageD1

				Expect(func() {
					packer.PackChildEvent(
						parent,
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
		var parent *Envelope

		BeforeEach(func() {
			parent = &Envelope{
				MetaData: MetaData{
					MessageID:     "<parent>",
					CausationID:   "<parent>",
					CorrelationID: "<parent>",
					Source: Source{
						Application: configkit.MustNewIdentity("<app-name>", "<app-key>"),
					},
				},
			}
		})

		DescribeTable(
			"it returns a new envelope",
			func(p dogma.Message) {
				parent.Message = p

				scheduledFor := time.Now()
				env := packer.PackChildTimeout(
					parent,
					MessageT1,
					scheduledFor,
					configkit.MustNewIdentity("<handler-name>", "<handler-key>"),
					"<instance>",
				)

				Expect(env).To(Equal(
					&Envelope{
						MetaData: MetaData{
							MessageID:     "00000001",
							CausationID:   "<parent>",
							CorrelationID: "<parent>",
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
			Entry("when the parent is an event", MessageF1),
			Entry("when the parent is a timeout", MessageU1),
		)

		DescribeTable(
			"it panics if the parent message type is incorrect",
			func(p dogma.Message, x string) {
				parent.Message = p

				Expect(func() {
					packer.PackChildTimeout(
						parent,
						MessageT1,
						time.Now(),
						configkit.MustNewIdentity("<handler-name>", "<handler-key>"),
						"<instance>",
					)
				}).To(PanicWith(x))
			},
			Entry("when the message is unrecognized", MessageX1, "fixtures.MessageX is not consumed by this handler"),
			Entry("when the message is a command", MessageD1, "fixtures.MessageD is a command, expected an event or timeout"),
		)

		DescribeTable(
			"it panics if the message type is incorrect",
			func(m dogma.Message, x string) {
				parent.Message = MessageF1

				Expect(func() {
					packer.PackChildTimeout(
						parent,
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
})
