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
	. "github.com/dogmatiq/infix/queue"
	. "github.com/dogmatiq/marshalkit/fixtures"
	"github.com/golang/protobuf/proto"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ dogma.CommandExecutor = (*CommandExecutor)(nil)

var _ = Describe("type Executor", func() {
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
			Queue: queue,
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
		if dataStore != nil {
			dataStore.Close()
		}

		cancel()
	})

	Describe("func ExecuteCommand()", func() {
		It("persists the message in the queue store", func() {
			err := executor.ExecuteCommand(ctx, MessageA1)
			Expect(err).ShouldNot(HaveOccurred())

			items, err := dataStore.QueueStoreRepository().LoadQueueMessages(ctx, 2)
			Expect(err).ShouldNot(HaveOccurred())
			Expect(items).To(HaveLen(1))

			x := &envelopespec.Envelope{
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
			}

			// TODO: https://github.com/dogmatiq/infix/issues/151
			i := items[0]
			if !proto.Equal(i.Envelope, x) {
				Expect(i.Envelope).To(Equal(x))
			}
		})

		It("schedules the message for immediate handling", func() {
			err := executor.ExecuteCommand(ctx, MessageA1)
			Expect(err).ShouldNot(HaveOccurred())

			items, err := dataStore.QueueStoreRepository().LoadQueueMessages(ctx, 2)
			Expect(err).ShouldNot(HaveOccurred())
			Expect(items).To(HaveLen(1))

			i := items[0]
			Expect(i.NextAttemptAt).To(BeTemporally("~", time.Now()))
		})

		It("returns an error if the transaction can not be begun", func() {
			dataStore.BeginFunc = func(
				ctx context.Context,
			) (persistence.Transaction, error) {
				return nil, errors.New("<error>")
			}

			err := executor.ExecuteCommand(ctx, MessageA1)
			Expect(err).Should(HaveOccurred())
		})
	})
})
