package queue_test

import (
	"context"
	"errors"
	"time"

	"github.com/dogmatiq/dogma"
	. "github.com/dogmatiq/enginekit/enginetest/stubs"
	"github.com/dogmatiq/enginekit/message"
	"github.com/dogmatiq/enginekit/protobuf/envelopepb"
	"github.com/dogmatiq/enginekit/protobuf/identitypb"
	"github.com/dogmatiq/enginekit/protobuf/uuidpb"
	. "github.com/dogmatiq/verity/fixtures"
	"github.com/dogmatiq/verity/persistence"
	. "github.com/dogmatiq/verity/queue"
	. "github.com/jmalloc/gomegax"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var _ dogma.CommandExecutor = (*CommandExecutor)(nil)

var _ = Describe("type CommandExecutor", func() {
	var (
		ctx       context.Context
		dataStore *DataStoreStub
		queue     *Queue
		loaded    chan struct{}
		executor  *CommandExecutor
	)

	BeforeEach(func() {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
		DeferCleanup(cancel)

		dataStore = NewDataStoreStub()
		DeferCleanup(dataStore.Close)

		loaded = make(chan struct{})
		dataStore.LoadQueueMessagesFunc = func(
			ctx context.Context,
			n int,
		) ([]persistence.QueueMessage, error) {
			defer close(loaded)
			return dataStore.DataStore.LoadQueueMessages(ctx, n)
		}

		queue = &Queue{
			Repository: dataStore,
		}

		executor = &CommandExecutor{
			Queue:     queue,
			Persister: dataStore,
			Packer: NewPacker(
				message.TypeFor[*CommandStub[TypeA]](),
			),
		}
	})

	JustBeforeEach(func() {
		q := queue
		go func() {
			defer GinkgoRecover()
			q.Run(ctx)
		}()
	})

	Describe("func ExecuteCommand()", func() {
		It("persists the message", func() {
			err := executor.ExecuteCommand(ctx, CommandA1)
			Expect(err).ShouldNot(HaveOccurred())

			dataStore.LoadQueueMessagesFunc = nil
			messages, err := dataStore.LoadQueueMessages(ctx, 2)
			Expect(err).ShouldNot(HaveOccurred())
			Expect(messages).To(EqualX(
				[]persistence.QueueMessage{
					{
						Revision:      1,
						NextAttemptAt: time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC),
						Envelope: &envelopepb.Envelope{
							MessageId:         uuidpb.Generate(), // TODO
							CorrelationId:     uuidpb.Generate(), // TODO
							CausationId:       uuidpb.Generate(), // TODO
							SourceApplication: identitypb.MustParse("<app-name>", DefaultAppKey),
							CreatedAt:         timestamppb.New(time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)),
							Description:       "command(stubs.TypeA:A1, valid)",
							TypeId:            uuidpb.Generate(), // TODO
							Data:              []byte(`{"content":"A1"}`),
						},
					},
				},
			))
		})

		It("adds the message to the queue", func() {
			select {
			case <-ctx.Done():
				Expect(ctx.Err()).ShouldNot(HaveOccurred())
			case <-loaded:
				// Wait until messages have already been loaded so that we know
				// the executor added the message to the queue directly, and it
				// wasn't just loaded from the repository.
			}

			err := executor.ExecuteCommand(ctx, CommandA1)
			Expect(err).ShouldNot(HaveOccurred())

			m, err := queue.Pop(ctx)
			Expect(err).ShouldNot(HaveOccurred())
			Expect(m.Parcel.Message).To(Equal(CommandA1))
		})

		It("returns an error if persistence fails", func() {
			dataStore.PersistFunc = func(
				context.Context,
				persistence.Batch,
			) (persistence.Result, error) {
				return persistence.Result{}, errors.New("<error>")
			}

			err := executor.ExecuteCommand(ctx, CommandA1)
			Expect(err).Should(HaveOccurred())
		})
	})
})
