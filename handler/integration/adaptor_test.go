package integration_test

import (
	"context"
	"errors"
	"time"

	. "github.com/dogmatiq/configkit/fixtures"
	"github.com/dogmatiq/configkit/message"
	"github.com/dogmatiq/dodeca/logging"
	"github.com/dogmatiq/dogma"
	. "github.com/dogmatiq/dogma/fixtures"
	"github.com/dogmatiq/infix/draftspecs/envelopespec"
	"github.com/dogmatiq/infix/eventstream"
	. "github.com/dogmatiq/infix/fixtures"
	"github.com/dogmatiq/infix/handler"
	. "github.com/dogmatiq/infix/handler/integration"
	"github.com/dogmatiq/infix/parcel"
	. "github.com/dogmatiq/marshalkit/fixtures"
	. "github.com/jmalloc/gomegax"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("type Adaptor", func() {
	var (
		dataStore *DataStoreStub
		integ     *IntegrationMessageHandler
		packer    *parcel.Packer
		logger    *logging.BufferedLogger
		work      *handler.UnitOfWork
		cause     *parcel.Parcel
		adaptor   *Adaptor
	)

	BeforeEach(func() {
		dataStore = NewDataStoreStub()

		integ = &IntegrationMessageHandler{
			ConfigureFunc: func(c dogma.IntegrationConfigurer) {
				c.Identity("<integration-name>", "<integration-key>")
				c.ConsumesCommandType(MessageC{})
				c.ProducesEventType(MessageE{})
			},
		}

		packer = NewPacker(
			message.TypeRoles{
				MessageCType: message.CommandRole,
				MessageEType: message.EventRole,
			},
		)

		logger = &logging.BufferedLogger{}

		work = &handler.UnitOfWork{}

		cause = NewParcel("<consume>", MessageC1)

		adaptor = &Adaptor{
			Identity: &envelopespec.Identity{
				Name: "<integration-name>",
				Key:  "<integration-key>",
			},
			Handler: integ,
			Packer:  packer,
			Logger:  logger,
		}
	})

	AfterEach(func() {
		dataStore.Close()
	})

	Describe("func Handle()", func() {
		It("forwards the message to the handler", func() {
			integ.HandleCommandFunc = func(
				_ context.Context,
				_ dogma.IntegrationCommandScope,
				m dogma.Message,
			) error {
				Expect(m).To(Equal(MessageC1))
				return errors.New("<error>")
			}

			err := adaptor.Handle(context.Background(), work, cause)
			Expect(err).To(MatchError("<error>"))
		})

		Context("when an event is recorded", func() {
			BeforeEach(func() {
				integ.HandleCommandFunc = func(
					_ context.Context,
					s dogma.IntegrationCommandScope,
					_ dogma.Message,
				) error {
					s.RecordEvent(MessageE1)
					return nil
				}

				err := adaptor.Handle(context.Background(), work, cause)
				Expect(err).ShouldNot(HaveOccurred())
			})

			It("adds the event to the unit-of-work", func() {
				work.Observe(func(r handler.Result, err error) {
					Expect(r.Events).To(EqualX(
						[]eventstream.Event{
							{
								Offset: 0,
								Parcel: &parcel.Parcel{
									Envelope: &envelopespec.Envelope{
										MetaData: &envelopespec.MetaData{
											MessageId:     "0",
											CausationId:   "<consume>",
											CorrelationId: "<correlation>",
											Source: &envelopespec.Source{
												Application: packer.Application,
												Handler:     adaptor.Identity,
											},
											CreatedAt:   "2000-01-01T00:00:00Z",
											Description: "{E1}",
										},
										PortableName: MessageEPortableName,
										MediaType:    MessageE1Packet.MediaType,
										Data:         MessageE1Packet.Data,
									},
									Message:   MessageE1,
									CreatedAt: time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC),
								},
							},
						},
					))
				})

				err := handler.Persist(
					context.Background(),
					dataStore,
					work,
				)
				Expect(err).ShouldNot(HaveOccurred())
			})

			It("logs about the event", func() {
				Expect(logger.Messages()).To(ContainElement(
					logging.BufferedLogMessage{
						Message: "= 0  ∵ <consume>  ⋲ <correlation>  ▲    MessageE ● {E1}",
					},
				))
			})
		})

		Context("when a message is logged via the scope", func() {
			BeforeEach(func() {
				integ.HandleCommandFunc = func(
					_ context.Context,
					s dogma.IntegrationCommandScope,
					_ dogma.Message,
				) error {
					s.Log("format %s", "<value>")
					return nil
				}

				err := adaptor.Handle(context.Background(), work, cause)
				Expect(err).ShouldNot(HaveOccurred())
			})

			It("logs using the standard format", func() {
				Expect(logger.Messages()).To(ContainElement(
					logging.BufferedLogMessage{
						Message: "= <consume>  ∵ <cause>  ⋲ <correlation>  ▼    MessageC ● format <value>",
					},
				))
			})
		})
	})
})
