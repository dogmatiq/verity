package aggregate

import (
	"time"

	. "github.com/dogmatiq/configkit/fixtures"
	"github.com/dogmatiq/configkit/message"
	"github.com/dogmatiq/dodeca/logging"
	"github.com/dogmatiq/dogma"
	. "github.com/dogmatiq/dogma/fixtures"
	"github.com/dogmatiq/infix/draftspecs/envelopespec"
	. "github.com/dogmatiq/infix/fixtures"
	"github.com/dogmatiq/infix/parcel"
	. "github.com/dogmatiq/marshalkit/fixtures"
	. "github.com/jmalloc/gomegax"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("type scope", func() {
	var (
		packer *parcel.Packer
		logger *logging.BufferedLogger
		root   *AggregateRoot
		sc     *scope
	)

	BeforeEach(func() {
		packer = NewPacker(
			message.TypeRoles{
				MessageCType: message.CommandRole,
				MessageEType: message.EventRole,
			},
		)

		logger = &logging.BufferedLogger{}

		root = &AggregateRoot{}

		sc = &scope{
			cause:  NewParcel("<consume>", MessageC1),
			packer: packer,
			handler: &envelopespec.Identity{
				Name: "<aggregate-name>",
				Key:  "<aggregate-key>",
			},
			logger: logger,
			id:     "<instance>",
			root:   root,
		}
	})

	Describe("func InstanceID()", func() {
		It("returns the instance ID that the message was routed to", func() {
			Expect(sc.InstanceID()).To(Equal("<instance>"))
		})
	})

	Describe("func Create()", func() {
		It("returns true when the instance does not already exist", func() {
			Expect(sc.Create()).To(BeTrue())
		})

		It("returns false when the instance already exists", func() {
			sc.exists = true
			Expect(sc.Create()).To(BeFalse())
		})

		It("returns false when the instance was already created via the same scope", func() {
			sc.Create()
			Expect(sc.Create()).To(BeFalse())
		})

		It("returns true if the instance was destroyed via the same scope", func() {
			sc.exists = true
			sc.Destroy()
			Expect(sc.Create()).To(BeTrue())
		})
	})

	Describe("func Destroy()", func() {
		It("can be called after the instance has been created", func() {
			sc.Create()
			sc.Destroy()
		})

		It("panics if the instance does not exist", func() {
			Expect(func() {
				sc.Destroy()
			}).To(PanicWith("can not destroy non-existent instance"))
		})
	})

	Describe("func Root()", func() {
		It("returns the aggregate root", func() {
			sc.Create()
			Expect(sc.Root()).To(BeIdenticalTo(root))
		})

		It("panics if the instance does not exist", func() {
			Expect(func() {
				sc.Root()
			}).To(PanicWith("can not access aggregate root of non-existent instance"))
		})
	})

	Describe("func RecordEvent()", func() {
		It("produces the correct envelope", func() {
			sc.Create()
			sc.RecordEvent(MessageE1)

			env := &envelopespec.Envelope{
				MetaData: &envelopespec.MetaData{
					MessageId:     "0",
					CausationId:   "<consume>",
					CorrelationId: "<correlation>",
					Source: &envelopespec.Source{
						Application: packer.Application,
						Handler:     sc.handler,
						InstanceId:  "<instance>",
					},
					CreatedAt:   "2000-01-01T00:00:00Z",
					Description: "{E1}",
				},
				PortableName: MessageEPortableName,
				MediaType:    MessageE1Packet.MediaType,
				Data:         MessageE1Packet.Data,
			}

			Expect(sc.events).To(EqualX(
				[]*parcel.Parcel{
					{
						Envelope:  env,
						Message:   MessageE1,
						CreatedAt: time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC),
					},
				},
			))
		})

		It("logs about recorded events", func() {
			sc.Create()
			sc.RecordEvent(MessageE1)

			Expect(logger.Messages()).To(ContainElement(
				logging.BufferedLogMessage{
					Message: "= 0  ∵ <consume>  ⋲ <correlation>  ▲    MessageE ● {E1}",
				},
			))
		})

		It("applies the event to the aggregate root", func() {
			called := false
			root.ApplyEventFunc = func(
				m dogma.Message,
				_ interface{},
			) {
				called = true
				Expect(m).To(Equal(MessageE1))
			}

			sc.Create()
			sc.RecordEvent(MessageE1)

			Expect(called).To(BeTrue())
		})

		It("panics if the instance does not exist", func() {
			Expect(func() {
				sc.RecordEvent(MessageE1)
			}).To(PanicWith("can not record event against non-existent instance"))
		})
	})

	Describe("func Log()", func() {
		It("logs using the standard format", func() {
			sc.Log("format %s", "<value>")

			Expect(logger.Messages()).To(ContainElement(
				logging.BufferedLogMessage{
					Message: "= <consume>  ∵ <cause>  ⋲ <correlation>  ▼    MessageC ● format <value>",
				},
			))
		})
	})
})
