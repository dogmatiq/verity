package pipeline_test

import (
	"context"
	"errors"

	"github.com/dogmatiq/dodeca/logging"
	. "github.com/dogmatiq/infix/fixtures"
	. "github.com/dogmatiq/infix/pipeline"
	. "github.com/dogmatiq/marshalkit/fixtures"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("func New()", func() {
	It("pushes the message down the pipeline", func() {
		marshaler := Marshaler
		logger := logging.DebugLogger
		sess := &SessionStub{}

		ep := New(
			marshaler,
			logger,
			Terminate(func(
				ctx context.Context,
				sc *Scope,
			) error {
				Expect(sc.Session).To(BeIdenticalTo(sess))
				Expect(sc.Marshaler).To(BeIdenticalTo(marshaler))
				Expect(sc.Logger).To(BeIdenticalTo(logger))
				return errors.New("<error>")
			}),
		)

		err := ep(context.Background(), sess)
		Expect(err).To(MatchError("<error>"))
	})
})
