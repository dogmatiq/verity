package pipeline_test

import (
	"context"
	"errors"
	"time"

	. "github.com/dogmatiq/dogma/fixtures"
	"github.com/dogmatiq/infix/draftspecs/envelopespec"
	. "github.com/dogmatiq/infix/fixtures"
	"github.com/dogmatiq/infix/parcel"
	"github.com/dogmatiq/infix/persistence/subsystem/eventstore"
	"github.com/dogmatiq/infix/persistence/subsystem/queuestore"
	. "github.com/dogmatiq/infix/pipeline"
	. "github.com/jmalloc/gomegax"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("type Response", func() {
	var (
		now time.Time
		pcl *parcel.Parcel
		tx  *TransactionStub
		res *Response
	)

	BeforeEach(func() {
		now = time.Now()
		pcl = NewParcel("<produce>", MessageP1, now, now)
		tx = &TransactionStub{}
		res = &Response{}
	})

	Describe("func EnqueueMessage()", func() {
		It("persists the message via the transaction", func() {
			called := false
			tx.SaveMessageToQueueFunc = func(
				_ context.Context,
				i *queuestore.Item,
			) error {
				called = true
				Expect(i).To(EqualX(
					&queuestore.Item{
						Revision:      0,
						NextAttemptAt: now,
						Envelope:      pcl.Envelope,
					},
				))
				return nil
			}

			err := res.EnqueueMessage(context.Background(), tx, pcl)
			Expect(err).ShouldNot(HaveOccurred())
			Expect(called).To(BeTrue())
		})

		It("sets the next-attempt time to now for commands", func() {
			pcl.ScheduledFor = time.Time{}

			tx.SaveMessageToQueueFunc = func(
				_ context.Context,
				i *queuestore.Item,
			) error {
				Expect(i.NextAttemptAt).To(BeTemporally("~", time.Now()))
				return nil
			}

			err := res.EnqueueMessage(context.Background(), tx, pcl)
			Expect(err).ShouldNot(HaveOccurred())
		})

		It("sets the next attempt time to the scheduled-at time for timeouts", func() {
			pcl.ScheduledFor = time.Now().Add(1 * time.Hour)

			tx.SaveMessageToQueueFunc = func(
				_ context.Context,
				i *queuestore.Item,
			) error {
				Expect(i.NextAttemptAt).To(BeTemporally("==", pcl.ScheduledFor))
				return nil
			}

			err := res.EnqueueMessage(context.Background(), tx, pcl)
			Expect(err).ShouldNot(HaveOccurred())
		})

		It("returns an error if the message can not be persisted", func() {
			tx.SaveMessageToQueueFunc = func(
				context.Context,
				*queuestore.Item,
			) error {
				return errors.New("<error>")
			}

			err := res.EnqueueMessage(context.Background(), tx, pcl)
			Expect(err).To(MatchError("<error>"))
		})
	})

	Describe("func RecordEvent()", func() {
		It("persists the event via the transaction", func() {
			called := false
			tx.SaveEventFunc = func(
				_ context.Context,
				env *envelopespec.Envelope,
			) (eventstore.Offset, error) {
				called = true
				Expect(env).To(EqualX(pcl.Envelope))
				return 0, nil
			}

			_, err := res.RecordEvent(context.Background(), tx, pcl)
			Expect(err).ShouldNot(HaveOccurred())
			Expect(called).To(BeTrue())
		})

		It("returns the offset", func() {
			tx.SaveEventFunc = func(
				context.Context,
				*envelopespec.Envelope,
			) (eventstore.Offset, error) {
				return 123, nil
			}

			o, err := res.RecordEvent(context.Background(), tx, pcl)
			Expect(err).ShouldNot(HaveOccurred())
			Expect(o).To(BeEquivalentTo(123))
		})

		It("returns an error if the message can not be persisted", func() {
			tx.SaveEventFunc = func(
				context.Context,
				*envelopespec.Envelope,
			) (eventstore.Offset, error) {
				return 0, errors.New("<error>")
			}

			_, err := res.RecordEvent(context.Background(), tx, pcl)
			Expect(err).To(MatchError("<error>"))
		})
	})
})
