package integration_test

import (
	"context"
	"errors"
	"time"

	"github.com/dogmatiq/configkit"
	. "github.com/dogmatiq/configkit/fixtures"
	"github.com/dogmatiq/configkit/message"
	"github.com/dogmatiq/dodeca/logging"
	"github.com/dogmatiq/dogma"
	. "github.com/dogmatiq/dogma/fixtures"
	"github.com/dogmatiq/infix/draftspecs/envelopespec"
	"github.com/dogmatiq/infix/envelope"
	. "github.com/dogmatiq/infix/fixtures"
	. "github.com/dogmatiq/infix/handler/integration"
	"github.com/dogmatiq/infix/persistence/subsystem/eventstore"
	"github.com/dogmatiq/infix/pipeline"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("type Sink", func() {
	var (
		env       *envelope.Envelope
		tx        *TransactionStub
		dataStore *DataStoreStub
		sess      *SessionStub
		scope     *pipeline.Scope
		handler   *IntegrationMessageHandler
		sink      *Sink
	)

	BeforeEach(func() {
		env = NewEnvelope("<id>", MessageC1)

		dataStore = NewDataStoreStub()
		scope, sess, tx = NewPipelineScope(env, dataStore)

		handler = &IntegrationMessageHandler{
			ConfigureFunc: func(c dogma.IntegrationConfigurer) {
				c.Identity("<integration-name>", "<integration-key>")
				c.ConsumesCommandType(MessageC{})
				c.ProducesEventType(MessageE{})
			},
		}

		sink = &Sink{
			Identity: &envelopespec.Identity{
				Name: "<integration-name>",
				Key:  "<integration-key>",
			},
			Handler: handler,
			Packer: NewPacker(
				message.TypeRoles{
					MessageCType: message.CommandRole,
					MessageEType: message.EventRole,
				},
			),
		}
	})

	AfterEach(func() {
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

			err := sink.Accept(context.Background(), scope)
			Expect(err).To(MatchError("<error>"))
		})

		It("returns an error if the message cannot be unpacked", func() {
			sess.MessageFunc = func() (dogma.Message, error) {
				return nil, errors.New("<error>")
			}

			err := sink.Accept(context.Background(), scope)
			Expect(err).To(MatchError("<error>"))
		})

		It("can record events via the scope", func() {
			handler.HandleCommandFunc = func(
				_ context.Context,
				s dogma.IntegrationCommandScope,
				_ dogma.Message,
			) error {
				s.RecordEvent(MessageE1)
				return nil
			}

			effect := &envelope.Envelope{
				MetaData: envelope.MetaData{
					MessageID:     "0",
					CausationID:   "<id>",
					CorrelationID: "<correlation>",
					Source: envelope.Source{
						Application: configkit.Identity{
							Name: "<app-name>",
							Key:  "<app-key>",
						},
						Handler: configkit.Identity{
							Name: "<integration-name>",
							Key:  "<integration-key>",
						},
					},
					CreatedAt: time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC),
				},
				Message: MessageE1,
			}

			err := sink.Accept(context.Background(), scope)
			Expect(err).ShouldNot(HaveOccurred())
			Expect(scope.Recorded).To(HaveLen(1))
			Expect(scope.Recorded[0].Original).To(Equal(effect))
		})

		It("logs about recorded events", func() {
			handler.HandleCommandFunc = func(
				_ context.Context,
				s dogma.IntegrationCommandScope,
				_ dogma.Message,
			) error {
				s.RecordEvent(MessageE1)
				return nil
			}

			err := sink.Accept(context.Background(), scope)
			Expect(err).ShouldNot(HaveOccurred())

			logger := scope.Logger.(*logging.BufferedLogger)
			Expect(logger.Messages()).To(ContainElement(
				logging.BufferedLogMessage{
					Message: "= 0  ∵ <id>  ⋲ <correlation>  ▲    MessageE ● {E1}",
				},
			))
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

			err := sink.Accept(context.Background(), scope)
			Expect(err).To(MatchError("<error>"))
		})

		It("can log messages via the scope", func() {
			handler.HandleCommandFunc = func(
				_ context.Context,
				s dogma.IntegrationCommandScope,
				_ dogma.Message,
			) error {
				s.Log("format %s", "<value>")
				return nil
			}

			err := sink.Accept(context.Background(), scope)
			Expect(err).ShouldNot(HaveOccurred())

			logger := scope.Logger.(*logging.BufferedLogger)
			Expect(logger.Messages()).To(ContainElement(
				logging.BufferedLogMessage{
					Message: "= <id>  ∵ <cause>  ⋲ <correlation>  ▼    MessageC ● format <value>",
				},
			))
		})
	})
})
