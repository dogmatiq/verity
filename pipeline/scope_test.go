package pipeline_test

import (
	"context"
	"errors"

	. "github.com/dogmatiq/dogma/fixtures"
	"github.com/dogmatiq/infix/draftspecs/envelopespec"
	"github.com/dogmatiq/infix/envelope"
	. "github.com/dogmatiq/infix/fixtures"
	"github.com/dogmatiq/infix/persistence/subsystem/eventstore"
	"github.com/dogmatiq/infix/persistence/subsystem/queuestore"
	. "github.com/dogmatiq/infix/pipeline"
	. "github.com/dogmatiq/marshalkit/fixtures"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("type Scope", func() {
	var (
		tx    *TransactionStub
		env   *envelope.Envelope
		scope *Scope
	)

	BeforeEach(func() {
		tx = &TransactionStub{}
		env = NewEnvelope("<id>", MessageA1)

		scope = &Scope{
			Tx:        tx,
			Marshaler: Marshaler,
		}
	})

	Describe("func EnqueueMessage()", func() {
		It("returns an error if the message can not be persisted", func() {
			tx.SaveMessageToQueueFunc = func(context.Context, *queuestore.Message) error {
				return errors.New("<error>")
			}

			err := scope.EnqueueMessage(context.Background(), env)
			Expect(err).To(MatchError("<error>"))
		})
	})

	Describe("func RecordEvent()", func() {
		It("returns an error if the message can not be persisted", func() {
			tx.SaveEventFunc = func(context.Context, *envelopespec.Envelope) (eventstore.Offset, error) {
				return 0, errors.New("<error>")
			}

			_, err := scope.RecordEvent(context.Background(), env)
			Expect(err).To(MatchError("<error>"))
		})
	})
})
