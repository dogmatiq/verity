package message_test

import (
	"fmt"
	"time"

	"github.com/dogmatiq/configkit"
	. "github.com/dogmatiq/configkit/fixtures"
	"github.com/dogmatiq/configkit/message"
	"github.com/dogmatiq/dogma"
	. "github.com/dogmatiq/dogma/fixtures"
	. "github.com/dogmatiq/infix/message"
	. "github.com/dogmatiq/marshalkit/fixtures"
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
			App:       configkit.MustNewIdentity("<app-name>", "<app-key>"),
			Marshaler: Marshaler,
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

	Describe("func NewPackerForApplication()", func() {
		It("returns a packer that uses the produced message roles", func() {
			app := &Application{
				ConfigureFunc: func(c dogma.ApplicationConfigurer) {
					c.Identity("<app-name>", "<app-key>")

					c.RegisterIntegration(&IntegrationMessageHandler{
						ConfigureFunc: func(c dogma.IntegrationConfigurer) {
							c.Identity("<integration-name>", "<integration-key>")
							c.ConsumesCommandType(MessageC{})
							c.ProducesEventType(MessageE{})
						},
					})
				},
			}

			cfg := configkit.FromApplication(app)

			p := NewPackerForApplication(cfg, Marshaler)
			Expect(p).To(Equal(&Packer{
				App: configkit.MustNewIdentity("<app-name>", "<app-key>"),
				Roles: message.TypeRoles{
					MessageEType: message.EventRole,
				},
				Marshaler: Marshaler,
			}))
		})
	})

	Describe("func NewCommand()", func() {
		It("returns a new envelope", func() {
			env := packer.PackCommand(MessageC1)

			Expect(env).To(Equal(
				&Envelope{
					MetaData: MetaData{
						MessageID:     "00000001",
						CausationID:   "00000001",
						CorrelationID: "00000001",
						Source: Source{
							App: configkit.MustNewIdentity("<app-name>", "<app-key>"),
						},
						CreatedAt: now,
					},
					Message: MessageC1,
					Packet:  MessageC1Packet,
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

	Describe("func NewEvent()", func() {
		It("returns a new envelope", func() {
			env := packer.PackEvent(MessageE1)

			Expect(env).To(Equal(
				&Envelope{
					MetaData: MetaData{
						MessageID:     "00000001",
						CausationID:   "00000001",
						CorrelationID: "00000001",
						Source: Source{
							App: configkit.MustNewIdentity("<app-name>", "<app-key>"),
						},
						CreatedAt: now,
					},
					Message: MessageE1,
					Packet:  MessageE1Packet,
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

	Describe("func NewChildCommand()", func() {
		var parent *Envelope

		BeforeEach(func() {
			parent = &Envelope{
				MetaData: MetaData{
					MessageID:     "<parent>",
					CausationID:   "<parent>",
					CorrelationID: "<parent>",
					Source: Source{
						App: configkit.MustNewIdentity("<app-name>", "<app-key>"),
					},
				},
				Message: MessageE1,
				Packet:  MessageE1Packet,
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
							App:        configkit.MustNewIdentity("<app-name>", "<app-key>"),
							Handler:    configkit.MustNewIdentity("<handler-name>", "<handler-key>"),
							InstanceID: "<instance>",
						},
						CreatedAt: now,
					},
					Message: MessageC1,
					Packet:  MessageC1Packet,
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

	Describe("func NewChildEvent()", func() {
		var parent *Envelope

		BeforeEach(func() {
			parent = &Envelope{
				MetaData: MetaData{
					MessageID:     "<parent>",
					CausationID:   "<parent>",
					CorrelationID: "<parent>",
					Source: Source{
						App: configkit.MustNewIdentity("<app-name>", "<app-key>"),
					},
				},
				Message: MessageC1,
				Packet:  MessageC1Packet,
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
							App:        configkit.MustNewIdentity("<app-name>", "<app-key>"),
							Handler:    configkit.MustNewIdentity("<handler-name>", "<handler-key>"),
							InstanceID: "<instance>",
						},
						CreatedAt: now,
					},
					Message: MessageE1,
					Packet:  MessageE1Packet,
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
