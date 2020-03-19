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
	. "github.com/dogmatiq/marshalkit/fixtures"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("type BoundPacker", func() {
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
			Marshaler:   Marshaler,
			Roles: message.TypeRoles{
				MessageCType: message.CommandRole,
				MessageDType: message.CommandRole,
				MessageEType: message.EventRole,
				MessageFType: message.EventRole,
				MessageTType: message.TimeoutRole,
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

	Describe("func PackChildCommand()", func() {
		var (
			parent *Envelope
			bound  *BoundPacker
		)

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
				Packet:  MessageE1Packet,
			}

			cfg := configkit.FromProcess(&ProcessMessageHandler{
				ConfigureFunc: func(c dogma.ProcessConfigurer) {
					c.Identity("<process-name>", "<process-key>")
					c.ConsumesEventType(MessageE{})
					c.ProducesCommandType(MessageC{})
				},
			})

			bound = packer.Bind(
				parent,
				cfg,
				"<instance>",
			)
		})

		It("returns a new envelope", func() {
			env := bound.PackChildCommand(MessageC1)

			Expect(env).To(Equal(
				&Envelope{
					MetaData: MetaData{
						MessageID:     "00000001",
						CausationID:   "<parent>",
						CorrelationID: "<parent>",
						Source: Source{
							Application: configkit.MustNewIdentity("<app-name>", "<app-key>"),
							Handler:     configkit.MustNewIdentity("<process-name>", "<process-key>"),
							InstanceID:  "<instance>",
						},
						CreatedAt: now,
					},
					Message: MessageC1,
					Packet:  MessageC1Packet,
				},
			))
		})

		It("panics if the message type is not produced by this handler", func() {
			Expect(func() {
				bound.PackChildCommand(MessageD1)
			}).To(Panic())
		})
	})

	Describe("func PackChildEvent()", func() {
		var (
			parent *Envelope
			bound  *BoundPacker
		)

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
				Packet:  MessageC1Packet,
			}

			cfg := configkit.FromAggregate(&AggregateMessageHandler{
				ConfigureFunc: func(c dogma.AggregateConfigurer) {
					c.Identity("<aggregate-name>", "<aggregate-key>")
					c.ConsumesCommandType(MessageC{})
					c.ProducesEventType(MessageE{})
				},
			})

			bound = packer.Bind(
				parent,
				cfg,
				"<instance>",
			)
		})

		It("returns a new envelope", func() {
			env := bound.PackChildEvent(MessageE1)

			Expect(env).To(Equal(
				&Envelope{
					MetaData: MetaData{
						MessageID:     "00000001",
						CausationID:   "<parent>",
						CorrelationID: "<parent>",
						Source: Source{
							Application: configkit.MustNewIdentity("<app-name>", "<app-key>"),
							Handler:     configkit.MustNewIdentity("<aggregate-name>", "<aggregate-key>"),
							InstanceID:  "<instance>",
						},
						CreatedAt: now,
					},
					Message: MessageE1,
					Packet:  MessageE1Packet,
				},
			))
		})

		It("panics if the message type is not produced by this handler", func() {
			Expect(func() {
				bound.PackChildEvent(MessageF1)
			}).To(Panic())
		})
	})

	Describe("func PackChildTimeout()", func() {
		var (
			parent *Envelope
			bound  *BoundPacker
		)

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
				Packet:  MessageE1Packet,
			}

			cfg := configkit.FromProcess(&ProcessMessageHandler{
				ConfigureFunc: func(c dogma.ProcessConfigurer) {
					c.Identity("<process-name>", "<process-key>")
					c.ConsumesEventType(MessageE{})
					c.ProducesCommandType(MessageC{})
					c.SchedulesTimeoutType(MessageT{})
				},
			})

			bound = packer.Bind(
				parent,
				cfg,
				"<instance>",
			)
		})

		It("returns a new envelope", func() {
			scheduledFor := time.Now()
			env := bound.PackChildTimeout(MessageT1, scheduledFor)

			Expect(env).To(Equal(
				&Envelope{
					MetaData: MetaData{
						MessageID:     "00000001",
						CausationID:   "<parent>",
						CorrelationID: "<parent>",
						Source: Source{
							Application: configkit.MustNewIdentity("<app-name>", "<app-key>"),
							Handler:     configkit.MustNewIdentity("<process-name>", "<process-key>"),
							InstanceID:  "<instance>",
						},
						CreatedAt:    now,
						ScheduledFor: scheduledFor,
					},
					Message: MessageT1,
					Packet:  MessageT1Packet,
				},
			))
		})

		It("panics if the message type is not produced by this handler", func() {
			Expect(func() {
				bound.PackChildTimeout(MessageU1, time.Now())
			}).To(Panic())
		})
	})
})
