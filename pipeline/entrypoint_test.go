package pipeline_test

import (
	"context"
	"errors"

	"github.com/dogmatiq/dodeca/logging"
	. "github.com/dogmatiq/dogma/fixtures"
	"github.com/dogmatiq/infix/envelope"
	. "github.com/dogmatiq/infix/fixtures"
	. "github.com/dogmatiq/infix/pipeline"
	. "github.com/dogmatiq/marshalkit/fixtures"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("func New()", func() {
	It("pushes the message down the pipeline", func() {
		tx := &TransactionStub{}
		marshaler := Marshaler
		logger := logging.DebugLogger
		env := NewEnvelope("<id>", MessageA1)

		ep := New(
			marshaler,
			logger,
			Terminate(func(
				ctx context.Context,
				sc *Scope,
				e *envelope.Envelope,
			) error {
				Expect(sc.Tx).To(Equal(tx))
				Expect(sc.Marshaler).To(BeIdenticalTo(marshaler))
				Expect(sc.Logger).To(BeIdenticalTo(logger))
				Expect(e).To(Equal(env))
				return errors.New("<error>")
			}),
		)

		err := ep(
			context.Background(),
			tx,
			env,
		)
		Expect(err).To(MatchError("<error>"))
	})
})
