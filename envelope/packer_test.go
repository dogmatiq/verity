package envelope_test

import (
	"fmt"
	"time"

	"github.com/dogmatiq/configkit"
	. "github.com/dogmatiq/configkit/fixtures"
	"github.com/dogmatiq/configkit/message"
	. "github.com/dogmatiq/dogma/fixtures"
	. "github.com/dogmatiq/infix/envelope"
	"github.com/google/uuid"
	. "github.com/onsi/ginkgo"
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
			Roles: message.TypeRoles{
				MessageCType: message.CommandRole,
				MessageEType: message.EventRole,
				MessageTType: message.TimeoutRole,
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

		It("panics if the message type is not recognized", func() {
			Expect(func() {
				packer.PackCommand(MessageA1)
			}).To(Panic())
		})

		It("panics if the message type has a different role", func() {
			Expect(func() {
				packer.PackCommand(MessageE1)
			}).To(Panic())
		})
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

		It("panics if the message type is not recognized", func() {
			Expect(func() {
				packer.PackEvent(MessageA1)
			}).To(Panic())
		})

		It("panics if the message type has a different role", func() {
			Expect(func() {
				packer.PackEvent(MessageC1)
			}).To(Panic())
		})
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
				Message: MessageE1,
			}
		})

		It("returns a new envelope", func() {
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
		})

		It("panics if the message type is not recognized", func() {
			Expect(func() {
				packer.PackChildCommand(
					parent,
					MessageA1,
					configkit.MustNewIdentity("<handler-name>", "<handler-key>"),
					"<instance>",
				)
			}).To(Panic())
		})

		It("panics if the message type has a different role", func() {
			Expect(func() {
				packer.PackChildCommand(
					parent,
					MessageE1,
					configkit.MustNewIdentity("<handler-name>", "<handler-key>"),
					"<instance>",
				)
			}).To(Panic())
		})
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
				Message: MessageC1,
			}
		})

		It("returns a new envelope", func() {
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
		})

		It("panics if the message type is not recognized", func() {
			Expect(func() {
				packer.PackChildEvent(
					parent,
					MessageA1,
					configkit.MustNewIdentity("<handler-name>", "<handler-key>"),
					"<instance>",
				)
			}).To(Panic())
		})

		It("panics if the message type has a different role", func() {
			Expect(func() {
				packer.PackChildEvent(
					parent,
					MessageC1,
					configkit.MustNewIdentity("<handler-name>", "<handler-key>"),
					"<instance>",
				)
			}).To(Panic())
		})
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
				Message: MessageE1,
			}
		})

		It("returns a new envelope", func() {
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
		})

		It("panics if the message type is not recognized", func() {
			Expect(func() {
				packer.PackChildTimeout(
					parent,
					MessageA1,
					time.Now(),
					configkit.MustNewIdentity("<handler-name>", "<handler-key>"),
					"<instance>",
				)
			}).To(Panic())
		})

		It("panics if the message type has a different role", func() {
			Expect(func() {
				packer.PackChildTimeout(
					parent,
					MessageE1,
					time.Now(),
					configkit.MustNewIdentity("<handler-name>", "<handler-key>"),
					"<instance>",
				)
			}).To(Panic())
		})
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
