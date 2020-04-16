package aggregate_test

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
	. "github.com/dogmatiq/infix/handler/aggregate"
	"github.com/dogmatiq/infix/parcel"
	"github.com/dogmatiq/infix/persistence"
	"github.com/dogmatiq/infix/persistence/subsystem/eventstore"
	"github.com/dogmatiq/infix/pipeline"
	. "github.com/dogmatiq/marshalkit/fixtures"
	"github.com/jmalloc/gomegax"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("type Sink", func() {
	var (
		tx        *TransactionStub
		dataStore *DataStoreStub
		req       *PipelineRequestStub
		res       *pipeline.Response
		handler   *AggregateMessageHandler
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

		handler = &AggregateMessageHandler{
			ConfigureFunc: func(c dogma.AggregateConfigurer) {
				c.Identity("<aggregate-name>", "<aggregate-key>")
				c.ConsumesCommandType(MessageC{})
				c.ProducesEventType(MessageE{})
			},
			RouteCommandToInstanceFunc: func(m dogma.Message) string {
				return "<instance>"
			},
		}

		logger = &logging.BufferedLogger{}

		sink = &Sink{
			Identity: &envelopespec.Identity{
				Name: "<aggregate-name>",
				Key:  "<aggregate-key>",
			},
			Handler: handler,
			Packer: NewPacker(
				message.TypeRoles{
					MessageCType: message.CommandRole,
					MessageEType: message.EventRole,
				},
			),
			Logger: logger,
		}
	})

	AfterEach(func() {
		if dataStore != nil {
			dataStore.Close()
		}
	})

	Describe("func Accept()", func() {
		It("forwards the message to the handler", func() {
			called := false
			handler.HandleCommandFunc = func(
				_ dogma.AggregateCommandScope,
				m dogma.Message,
			) {
				called = true
				Expect(m).To(Equal(MessageC1))
			}

			err := sink.Accept(context.Background(), req, res)
			Expect(err).ShouldNot(HaveOccurred())
			Expect(called).To(BeTrue())
		})

		It("returns an error if the message cannot be unpacked", func() {
			req.ParcelFunc = func() (*parcel.Parcel, error) {
				return nil, errors.New("<error>")
			}

			err := sink.Accept(context.Background(), req, res)
			Expect(err).To(MatchError("<error>"))
		})

		It("logs about recorded events", func() {
			handler.HandleCommandFunc = func(
				s dogma.AggregateCommandScope,
				_ dogma.Message,
			) {
				s.RecordEvent(MessageE1)
			}

			err := sink.Accept(context.Background(), req, res)
			Expect(err).ShouldNot(HaveOccurred())
			Expect(logger.Messages()).To(ContainElement(
				logging.BufferedLogMessage{
					Message: "= 0  ∵ <consume>  ⋲ <correlation>  ▲    MessageE ● {E1}",
				},
			))
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
				s dogma.AggregateCommandScope,
				_ dogma.Message,
			) {
				s.RecordEvent(MessageE1)
			}

			err := sink.Accept(context.Background(), req, res)
			Expect(err).To(MatchError("<error>"))
		})

		Describe("type scope", func() {
			Describe("func InstanceID()", func() {
				It("returns the instance ID that the message was routed to", func() {
					handler.HandleCommandFunc = func(
						s dogma.AggregateCommandScope,
						_ dogma.Message,
					) {
						Expect(s.InstanceID()).To(Equal("<instance>"))
					}

					err := sink.Accept(context.Background(), req, res)
					Expect(err).ShouldNot(HaveOccurred())
				})
			})

			Describe("func RecordEvent()", func() {
				It("produces the correct envelope", func() {
					handler.HandleCommandFunc = func(
						s dogma.AggregateCommandScope,
						_ dogma.Message,
					) {
						s.RecordEvent(MessageE1)
					}

					err := sink.Accept(context.Background(), req, res)
					Expect(err).ShouldNot(HaveOccurred())

					env := &envelopespec.Envelope{
						MetaData: &envelopespec.MetaData{
							MessageId:     "0",
							CausationId:   "<consume>",
							CorrelationId: "<correlation>",
							Source: &envelopespec.Source{
								Application: sink.Packer.Application,
								Handler:     sink.Identity,
								InstanceId:  "<instance>",
							},
							CreatedAt:   "2000-01-01T00:00:00Z",
							Description: "{E1}",
						},
						PortableName: MessageEPortableName,
						MediaType:    MessageE1Packet.MediaType,
						Data:         MessageE1Packet.Data,
					}

					Expect(res.RecordedEvents).To(gomegax.EqualX(
						[]pipeline.RecordedEvent{
							{
								Parcel: &parcel.Parcel{
									Envelope:  env,
									Message:   MessageE1,
									CreatedAt: time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC),
								},
								Persisted: &eventstore.Item{
									Offset:   0,
									Envelope: env,
								},
							},
						},
					))
				})
			})

			Describe("func RecordEvent()", func() {
				It("logs using the standard format", func() {
					handler.HandleCommandFunc = func(
						s dogma.AggregateCommandScope,
						_ dogma.Message,
					) {
						s.Log("format %s", "<value>")
					}

					err := sink.Accept(context.Background(), req, res)
					Expect(err).ShouldNot(HaveOccurred())
					Expect(logger.Messages()).To(ContainElement(
						logging.BufferedLogMessage{
							Message: "= <consume>  ∵ <cause>  ⋲ <correlation>  ▼    MessageC ● format <value>",
						},
					))
				})
			})
		})
	})
})
