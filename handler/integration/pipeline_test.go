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
	. "github.com/dogmatiq/infix/fixtures"
	. "github.com/dogmatiq/infix/handler/integration"
	"github.com/dogmatiq/infix/parcel"
	"github.com/dogmatiq/infix/persistence"
	"github.com/dogmatiq/infix/persistence/subsystem/eventstore"
	"github.com/dogmatiq/infix/pipeline"
	. "github.com/dogmatiq/marshalkit/fixtures"
	. "github.com/jmalloc/gomegax"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("type Sink", func() {
	var (
		tx        *TransactionStub
		dataStore *DataStoreStub
		req       *PipelineRequestStub
		res       *pipeline.Response
		handler   *IntegrationMessageHandler
		packer    *parcel.Packer
		logger    *logging.BufferedLogger
		sink      *Sink
	)

	BeforeEach(func() {
		dataStore = NewDataStoreStub()

		req, tx = NewPipelineRequestStub(
			NewParcel("<consume>", MessageC1),
			dataStore,
		)
		res = &pipeline.Response{}

		handler = &IntegrationMessageHandler{
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

		sink = &Sink{
			Identity: &envelopespec.Identity{
				Name: "<integration-name>",
				Key:  "<integration-key>",
			},
			Handler: handler,
			Packer:  packer,
			Logger:  logger,
		}
	})

	AfterEach(func() {
		if req != nil {
			req.Close()
		}

		if dataStore != nil {
			dataStore.Close()
		}
	})

	Describe("func Accept()", func() {
		It("forwards the message to the handler", func() {
			handler.HandleCommandFunc = func(
				_ context.Context,
				_ dogma.IntegrationCommandScope,
				m dogma.Message,
			) error {
				Expect(m).To(Equal(MessageC1))
				return errors.New("<error>")
			}

			err := sink.Accept(context.Background(), req, res)
			Expect(err).To(MatchError("<error>"))
		})

		It("saves the recorded events", func() {
			handler.HandleCommandFunc = func(
				_ context.Context,
				s dogma.IntegrationCommandScope,
				_ dogma.Message,
			) error {
				s.RecordEvent(MessageE1)
				return nil
			}

			ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
			defer cancel()

			err := sink.Accept(ctx, req, res)
			Expect(err).ShouldNot(HaveOccurred())

			err = req.Ack(ctx)
			Expect(err).ShouldNot(HaveOccurred())

			res, err := dataStore.EventStoreRepository().QueryEvents(ctx, eventstore.Query{})
			defer res.Close()

			i, ok, err := res.Next(ctx)
			Expect(err).ShouldNot(HaveOccurred())
			Expect(ok).To(BeTrue())
			Expect(i).To(EqualX(
				&eventstore.Item{
					Offset: 0,
					Envelope: &envelopespec.Envelope{
						MetaData: &envelopespec.MetaData{
							MessageId:     "0",
							CausationId:   "<consume>",
							CorrelationId: "<correlation>",
							Source: &envelopespec.Source{
								Application: packer.Application,
								Handler:     sink.Identity,
							},
							CreatedAt:   "2000-01-01T00:00:00Z",
							Description: "{E1}",
						},
						PortableName: MessageEPortableName,
						MediaType:    MessageE1Packet.MediaType,
						Data:         MessageE1Packet.Data,
					},
				},
			))
		})

		It("returns an error if the message cannot be unpacked", func() {
			req.ParcelFunc = func() (*parcel.Parcel, error) {
				return nil, errors.New("<error>")
			}

			err := sink.Accept(context.Background(), req, res)
			Expect(err).To(MatchError("<error>"))
		})

		It("returns an error if the transaction cannot be started", func() {
			req.TxFunc = func(
				context.Context,
			) (persistence.ManagedTransaction, error) {
				return nil, errors.New("<error>")
			}

			err := sink.Accept(context.Background(), req, res)
			Expect(err).To(MatchError("<error>"))
		})

		It("returns an error if an event can not be recorded", func() {
			tx.SaveEventFunc = func(
				context.Context,
				*envelopespec.Envelope,
			) (eventstore.Offset, error) {
				return 0, errors.New("<error>")
			}

			handler.HandleCommandFunc = func(
				_ context.Context,
				s dogma.IntegrationCommandScope,
				_ dogma.Message,
			) error {
				s.RecordEvent(MessageE1)
				return nil
			}

			err := sink.Accept(context.Background(), req, res)
			Expect(err).To(MatchError("<error>"))
		})
	})
})
