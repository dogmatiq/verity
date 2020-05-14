package queue_test

import (
	"context"
	"errors"
	"time"

	"github.com/dogmatiq/configkit/message"
	"github.com/dogmatiq/dogma"
	. "github.com/dogmatiq/dogma/fixtures"
	"github.com/dogmatiq/infix/draftspecs/envelopespec"
	. "github.com/dogmatiq/infix/fixtures"
	"github.com/dogmatiq/infix/persistence"
	"github.com/dogmatiq/infix/persistence/subsystem/queuestore"
	. "github.com/dogmatiq/infix/queue"
	. "github.com/dogmatiq/marshalkit/fixtures"
	. "github.com/jmalloc/gomegax"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ dogma.CommandExecutor = (*CommandExecutor)(nil)

var _ = Describe("type CommandExecutor", func() {
	var (
		ctx       context.Context
		cancel    context.CancelFunc
		dataStore *DataStoreStub
		queue     *Queue
		executor  *CommandExecutor
	)

	BeforeEach(func() {
		ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)

		dataStore = NewDataStoreStub()

		queue = &Queue{
			DataStore: dataStore,
			Marshaler: Marshaler,
		}

		executor = &CommandExecutor{
			Queue:     queue,
			Persister: dataStore,
			Packer: NewPacker(
				message.TypeRoles{
					message.TypeOf(MessageA{}): message.CommandRole,
				},
			),
		}
	})

	JustBeforeEach(func() {
		go queue.Run(ctx)
	})

	AfterEach(func() {
		dataStore.Close()
		cancel()
	})

	Describe("func ExecuteCommand()", func() {
		It("persists the message", func() {
			err := executor.ExecuteCommand(ctx, MessageA1)
			Expect(err).ShouldNot(HaveOccurred())

			items, err := dataStore.QueueStoreRepository().LoadQueueMessages(ctx, 2)
			Expect(err).ShouldNot(HaveOccurred())
			Expect(items).To(EqualX(
				[]*queuestore.Item{
					{
						Revision:      1,
						NextAttemptAt: time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC),
						Envelope: &envelopespec.Envelope{
							MetaData: &envelopespec.MetaData{
								MessageId:     "0",
								CorrelationId: "0",
								CausationId:   "0",
								Source: &envelopespec.Source{
									Application: &envelopespec.Identity{
										Name: "<app-name>",
										Key:  "<app-key>",
									},
								},
								CreatedAt:   "2000-01-01T00:00:00Z",
								Description: "{A1}",
							},
							PortableName: MessageAPortableName,
							MediaType:    MessageA1Packet.MediaType,
							Data:         MessageA1Packet.Data,
						},
					},
				},
			))
		})

		It("returns an error if persistence fails", func() {
			dataStore.PersistFunc = func(
				context.Context,
				persistence.Batch,
			) (persistence.Result, error) {
				return persistence.Result{}, errors.New("<error>")
			}

			err := executor.ExecuteCommand(ctx, MessageA1)
			Expect(err).Should(HaveOccurred())
		})
	})
})
