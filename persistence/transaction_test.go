package persistence_test

import (
	"context"
	"errors"
	"time"

	. "github.com/dogmatiq/dogma/fixtures"
	. "github.com/dogmatiq/infix/fixtures"
	. "github.com/dogmatiq/infix/persistence"
	"github.com/dogmatiq/infix/persistence/provider/memory"
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
		env := NewEnvelopeProto("<id>", MessageA1)

		err := WithTransaction(
			ctx,
			dataStore,
			func(tx Transaction) error {
				return tx.SaveMessageToQueue(ctx, env, time.Now())
			},
		)
		Expect(err).ShouldNot(HaveOccurred())

		messages, err := dataStore.QueueStoreRepository().LoadQueueMessages(ctx, 1)
		Expect(err).ShouldNot(HaveOccurred())
		Expect(messages).NotTo(BeEmpty())
	})

	It("rolls the transaction back fn returns an error", func() {
		env := NewEnvelopeProto("<id>", MessageA1)

		err := WithTransaction(
			ctx,
			dataStore,
			func(tx Transaction) error {
				err := tx.SaveMessageToQueue(ctx, env, time.Now())
				Expect(err).ShouldNot(HaveOccurred())

				return errors.New("<error>")
			},
		)
		Expect(err).To(MatchError("<error>"))

		messages, err := dataStore.QueueStoreRepository().LoadQueueMessages(ctx, 1)
		Expect(err).ShouldNot(HaveOccurred())
		Expect(messages).To(BeEmpty())
	})

	It("returns an error if the transaction can not be begun", func() {
		dataStore.Close()

		err := WithTransaction(
			ctx,
			dataStore,
			func(Transaction) error {
				Fail("unexpectedly invoked fn()")
				return nil
			},
		)
		Expect(err).To(Equal(ErrDataStoreClosed))
	})
})
