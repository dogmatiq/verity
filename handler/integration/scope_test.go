package integration

import (
	"time"

	. "github.com/dogmatiq/configkit/fixtures"
	"github.com/dogmatiq/configkit/message"
	"github.com/dogmatiq/dodeca/logging"
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

		sc = &scope{
			cause:  NewParcel("<consume>", MessageC1),
			packer: packer,
			handler: &envelopespec.Identity{
				Name: "<integration-name>",
				Key:  "<integration-key>",
			},
			logger: logger,
		}
	})

	Describe("func RecordEvent()", func() {
		It("produces the correct envelope", func() {
			sc.RecordEvent(MessageE1)

			env := &envelopespec.Envelope{
				MetaData: &envelopespec.MetaData{
					MessageId:     "0",
					CausationId:   "<consume>",
					CorrelationId: "<correlation>",
					Source: &envelopespec.Source{
						Application: packer.Application,
						Handler:     sc.handler,
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
			sc.RecordEvent(MessageE1)

			Expect(logger.Messages()).To(ContainElement(
				logging.BufferedLogMessage{
					Message: "= 0  ∵ <consume>  ⋲ <correlation>  ▲    MessageE ● {E1}",
				},
			))
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
