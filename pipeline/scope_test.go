package pipeline_test

import (
	"context"
	"errors"

	. "github.com/dogmatiq/dogma/fixtures"
	"github.com/dogmatiq/infix/draftspecs/envelopespec"
	. "github.com/dogmatiq/infix/fixtures"
	"github.com/dogmatiq/infix/parcel"
	"github.com/dogmatiq/infix/persistence"
	"github.com/dogmatiq/infix/persistence/subsystem/eventstore"
	"github.com/dogmatiq/infix/persistence/subsystem/queuestore"
	. "github.com/dogmatiq/infix/pipeline"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("type Scope", func() {
	var (
		pcl   *parcel.Parcel
		tx    *TransactionStub
		sess  *SessionStub
		scope *Scope
	)

	BeforeEach(func() {
		pcl = NewParcel("<produce>", MessageP1)

		scope, sess, tx = NewPipelineScope(
			NewEnvelope("<consume>", MessageC1),
			nil,
		)
	})

	Describe("func EnqueueMessage()", func() {
		XIt("persists the message", func() {
			// TODO:
		})

		XIt("sets the next attempt time to now", func() {
			// TODO:
		})

		It("returns an error if the transaction can not be started", func() {
			sess.TxFunc = func(context.Context) (persistence.ManagedTransaction, error) {
				return nil, errors.New("<error>")
			}

			err := scope.EnqueueMessage(context.Background(), pcl)
			Expect(err).To(MatchError("<error>"))
		})

		It("returns an error if the message can not be persisted", func() {
			tx.SaveMessageToQueueFunc = func(context.Context, *queuestore.Item) error {
				return errors.New("<error>")
			}

			err := scope.EnqueueMessage(context.Background(), pcl)
			Expect(err).To(MatchError("<error>"))
		})
	})

	Describe("func RecordEvent()", func() {
		XIt("persists the event", func() {
			// TODO:
		})

		XIt("returns the offset", func() {
			// TODO:
		})

		It("returns an error if the transaction can not be started", func() {
			sess.TxFunc = func(context.Context) (persistence.ManagedTransaction, error) {
				return nil, errors.New("<error>")
			}

			_, err := scope.RecordEvent(context.Background(), pcl)
			Expect(err).To(MatchError("<error>"))
		})

		It("returns an error if the message can not be persisted", func() {
			tx.SaveEventFunc = func(context.Context, *envelopespec.Envelope) (eventstore.Offset, error) {
				return 0, errors.New("<error>")
			}

			_, err := scope.RecordEvent(context.Background(), pcl)
			Expect(err).To(MatchError("<error>"))
		})
	})
})
