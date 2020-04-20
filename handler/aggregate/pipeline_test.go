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
	"github.com/dogmatiq/infix/persistence/subsystem/aggregatestore"
	"github.com/dogmatiq/infix/persistence/subsystem/eventstore"
	"github.com/dogmatiq/infix/pipeline"
	. "github.com/dogmatiq/marshalkit/fixtures"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("type Sink", func() {
	var (
		ctx       context.Context
		cancel    context.CancelFunc
		tx        *TransactionStub
		dataStore *DataStoreStub
		req       *PipelineRequestStub
		res       *pipeline.Response
		handler   *AggregateMessageHandler
		packer    *parcel.Packer
		logger    *logging.BufferedLogger
		sink      *Sink
	)

	BeforeEach(func() {
		ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)

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

		packer = NewPacker(
			message.TypeRoles{
				MessageCType: message.CommandRole,
				MessageEType: message.EventRole,
			},
		)

		logger = &logging.BufferedLogger{}

		sink = &Sink{
			Identity: &envelopespec.Identity{
				Name: "<aggregate-name>",
				Key:  "<aggregate-key>",
			},
			Handler:        handler,
			AggregateStore: dataStore.AggregateStoreRepository(),
			EventStore:     dataStore.EventStoreRepository(),
			Marshaler:      Marshaler,
			Packer:         packer,
			Logger:         logger,
		}
	})

	AfterEach(func() {
		if dataStore != nil {
			dataStore.Close()
		}

		cancel()
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

			err := sink.Accept(ctx, req, res)
			Expect(err).ShouldNot(HaveOccurred())
			Expect(called).To(BeTrue())
		})

		It("returns an error if the message cannot be unpacked", func() {
			req.ParcelFunc = func() (*parcel.Parcel, error) {
				return nil, errors.New("<error>")
			}

			err := sink.Accept(ctx, req, res)
			Expect(err).To(MatchError("<error>"))
		})

		When("events are recorded", func() {
			BeforeEach(func() {
				handler.HandleCommandFunc = func(
					s dogma.AggregateCommandScope,
					_ dogma.Message,
				) {
					s.Create()
					s.RecordEvent(MessageE1)
					s.RecordEvent(MessageE2)
				}
			})

			It("returns an error if the transaction cannot be started", func() {
				req.TxFunc = func(
					context.Context,
				) (persistence.ManagedTransaction, error) {
					return nil, errors.New("<error>")
				}

				err := sink.Accept(ctx, req, res)
				Expect(err).To(MatchError("<error>"))
			})

			It("returns an error if an event can not be recorded", func() {
				tx.SaveEventFunc = func(
					context.Context,
					*envelopespec.Envelope,
				) (eventstore.Offset, error) {
					return 0, errors.New("<error>")
				}

				err := sink.Accept(ctx, req, res)
				Expect(err).To(MatchError("<error>"))
			})

			It("returns an error if the meta-data can not be saved", func() {
				tx.SaveAggregateMetaDataFunc = func(
					context.Context,
					*aggregatestore.MetaData,
				) error {
					return errors.New("<error>")
				}

				err := sink.Accept(ctx, req, res)
				Expect(err).To(MatchError("<error>"))
			})
		})

		When("no events are recorded", func() {
			It("it does not start the transaction", func() {
				req.TxFunc = func(
					context.Context,
				) (persistence.ManagedTransaction, error) {
					Fail("unexpected call")
					return nil, nil
				}

				sink.Accept(ctx, req, res)
			})
		})
	})
})
