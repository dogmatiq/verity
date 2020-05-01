package persistence_test

import (
	"context"
	"errors"
	"time"

	. "github.com/dogmatiq/dogma/fixtures"
	. "github.com/dogmatiq/infix/fixtures"
	. "github.com/dogmatiq/infix/persistence"
	"github.com/dogmatiq/infix/persistence/provider/memory"
	"github.com/dogmatiq/infix/persistence/subsystem/eventstore"
	"github.com/dogmatiq/infix/persistence/subsystem/queuestore"
	. "github.com/jmalloc/gomegax"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("func WithTransaction", func() {
	var (
		ctx       context.Context
		cancel    context.CancelFunc
		provider  Provider
		dataStore DataStore
	)

	BeforeEach(func() {
		ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)

		provider = &memory.Provider{}

		var err error
		dataStore, err = provider.Open(ctx, "<app-key>")
		Expect(err).ShouldNot(HaveOccurred())
	})

	AfterEach(func() {
		cancel()

		if dataStore != nil {
			dataStore.Close()
		}
	})

	It("commits the transaction if fn returns nil", func() {
		env := NewEnvelope("<id>", MessageA1)

		_, err := WithTransaction(
			ctx,
			dataStore,
			func(tx ManagedTransaction) error {
				return tx.SaveMessageToQueue(
					ctx,
					&queuestore.Item{
						NextAttemptAt: time.Now(),
						Envelope:      env,
					},
				)
			},
		)
		Expect(err).ShouldNot(HaveOccurred())

		items, err := dataStore.QueueStoreRepository().LoadQueueMessages(ctx, 1)
		Expect(err).ShouldNot(HaveOccurred())
		Expect(items).NotTo(BeEmpty())
	})

	It("includes event store items in the transaction result", func() {
		env1 := NewEnvelope("<id-1>", MessageA1)
		env2 := NewEnvelope("<id-2>", MessageA2)
		env3 := NewEnvelope("<id-3>", MessageA3)

		result, err := WithTransaction(
			ctx,
			dataStore,
			func(tx ManagedTransaction) error {
				if _, err := tx.SaveEvent(
					ctx,
					env1,
				); err != nil {
					return err
				}

				if _, err := tx.SaveEvent(
					ctx,
					env2,
				); err != nil {
					return err
				}

				_, err := tx.SaveEvent(
					ctx,
					env3,
				)
				return err
			},
		)

		Expect(err).ShouldNot(HaveOccurred())
		Expect(result.EventItems).NotTo(BeEmpty())
		Expect(result.EventItems).To(
			EqualX([]*eventstore.Item{
				{
					Offset:   0,
					Envelope: env1,
				},
				{
					Offset:   1,
					Envelope: env2,
				},
				{
					Offset:   2,
					Envelope: env3,
				},
			}),
		)
	})

	It("rolls the transaction back if fn returns an error", func() {
		env := NewEnvelope("<id>", MessageA1)

		_, err := WithTransaction(
			ctx,
			dataStore,
			func(tx ManagedTransaction) error {
				err := tx.SaveMessageToQueue(
					ctx,
					&queuestore.Item{
						NextAttemptAt: time.Now(),
						Envelope:      env,
					},
				)
				Expect(err).ShouldNot(HaveOccurred())

				return errors.New("<error>")
			},
		)
		Expect(err).To(MatchError("<error>"))

		items, err := dataStore.QueueStoreRepository().LoadQueueMessages(ctx, 1)
		Expect(err).ShouldNot(HaveOccurred())
		Expect(items).To(BeEmpty())
	})

	It("returns an error if the transaction can not be begun", func() {
		dataStore.Close()

		_, err := WithTransaction(
			ctx,
			dataStore,
			func(ManagedTransaction) error {
				Fail("unexpectedly invoked fn()")
				return nil
			},
		)
		Expect(err).To(Equal(ErrDataStoreClosed))
	})
})
