package pipeline_test

import (
	"context"
	"errors"

	. "github.com/dogmatiq/dogma/fixtures"
	"github.com/dogmatiq/infix/draftspecs/envelopespec"
	. "github.com/dogmatiq/infix/fixtures"
	"github.com/dogmatiq/infix/parcel"
	"github.com/dogmatiq/infix/persistence/subsystem/eventstore"
	"github.com/dogmatiq/infix/persistence/subsystem/queuestore"
	. "github.com/dogmatiq/infix/pipeline"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("type Response", func() {
	var (
		pcl *parcel.Parcel
		tx  *TransactionStub
		res *Response
	)

	BeforeEach(func() {
		pcl = NewParcel("<produce>", MessageP1)
		tx = &TransactionStub{}
		res = &Response{}
	})

	Describe("func EnqueueMessage()", func() {
		XIt("persists the message", func() {
			// TODO:
		})

		XIt("sets the next attempt time to now", func() {
			// TODO:
		})

		It("returns an error if the message can not be persisted", func() {
			tx.SaveMessageToQueueFunc = func(context.Context, *queuestore.Item) error {
				return errors.New("<error>")
			}

			err := res.EnqueueMessage(context.Background(), tx, pcl)
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

		It("returns an error if the message can not be persisted", func() {
			tx.SaveEventFunc = func(context.Context, *envelopespec.Envelope) (eventstore.Offset, error) {
				return 0, errors.New("<error>")
			}

			_, err := res.RecordEvent(context.Background(), tx, pcl)
			Expect(err).To(MatchError("<error>"))
		})
	})
})
