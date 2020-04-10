package pipeline_test

import (
	"context"
	"errors"

	"github.com/dogmatiq/dodeca/logging"
	. "github.com/dogmatiq/dogma/fixtures"
	"github.com/dogmatiq/infix/envelope"
	. "github.com/dogmatiq/infix/fixtures"
	"github.com/dogmatiq/infix/handler"
	"github.com/dogmatiq/infix/persistence"
	. "github.com/dogmatiq/infix/pipeline"
	. "github.com/dogmatiq/marshalkit/fixtures"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("func Handle", func() {
	var (
		tx      *TransactionStub
		session *SessionStub
		scope   *Scope
		env     *envelope.Envelope
	)

	BeforeEach(func() {
		env = NewEnvelope("<id>", MessageA1)

		tx = &TransactionStub{}

		session = &SessionStub{
			TxFunc: func(ctx context.Context) (persistence.ManagedTransaction, error) {
				return tx, nil
			},
		}

		session.EnvelopeFunc = func() (*envelope.Envelope, error) {
			return env, nil
		}

		scope = &Scope{
			Session:   session,
			Marshaler: Marshaler,
		}
	})

	It("invokes the message handler", func() {
		called := false
		h := func(
			_ context.Context,
			_ handler.Scope,
			e *envelope.Envelope,
		) error {
			called = true
			Expect(e).To(Equal(env))
			return nil
		}

		stage := Handle(h)

		err := stage(context.Background(), scope)
		Expect(err).ShouldNot(HaveOccurred())
		Expect(called).To(BeTrue())
	})

	It("returns an error if the envelope cannot be unpacked", func() {
		session.EnvelopeFunc = func() (*envelope.Envelope, error) {
			return nil, errors.New("<error>")
		}

		h := func(
			context.Context,
			handler.Scope,
			*envelope.Envelope,
		) error {
			Fail("unexpected call")
			return nil
		}

		stage := Handle(h)

		err := stage(context.Background(), scope)
		Expect(err).To(MatchError("<error>"))
	})

	It("can obtain the session transaction via the scope", func() {
		h := func(
			ctx context.Context,
			sc handler.Scope,
			_ *envelope.Envelope,
		) error {
			t, err := sc.Tx(ctx)
			Expect(err).ShouldNot(HaveOccurred())
			Expect(t).To(BeIdenticalTo(tx))
			return nil
		}

		stage := Handle(h)

		err := stage(context.Background(), scope)
		Expect(err).ShouldNot(HaveOccurred())
	})

	It("can log messages via the scope", func() {
		logger := &logging.BufferedLogger{}
		scope.Logger = logger

		h := func(
			ctx context.Context,
			sc handler.Scope,
			_ *envelope.Envelope,
		) error {
			sc.Log("<format %d>", 1)
			return nil
		}

		stage := Handle(h)

		err := stage(context.Background(), scope)
		Expect(err).ShouldNot(HaveOccurred())
		Expect(logger.Messages()).To(ConsistOf(
			logging.BufferedLogMessage{
				Message: "= <id>  ∵ <cause>  ⋲ <correlation>  ▼ ⨝  fixtures.MessageA ● <format 1>",
			},
		))
	})
})
