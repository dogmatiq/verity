package pipeline_test

import (
	"context"
	"errors"
	"time"

	. "github.com/dogmatiq/infix/fixtures"
	. "github.com/dogmatiq/infix/pipeline"
	"github.com/dogmatiq/linger/backoff"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("func Acknowledge()", func() {
	var (
		session *SessionStub
		scope   *Scope
	)

	BeforeEach(func() {
		session = &SessionStub{}

		scope = &Scope{
			Session: session,
		}
	})

	It("acknowledges the session if the next stage succeeds", func() {
		session.AckFunc = func(context.Context) error {
			return errors.New("<error>")
		}

		stage := Acknowledge(nil)

		err := stage(context.Background(), scope, pass)
		Expect(err).To(MatchError("<error>"))
	})

	It("negatively acknowledges the session if the next stage fails", func() {
		session.NackFunc = func(_ context.Context, n time.Time) error {
			Expect(n).To(BeTemporally("~", time.Now().Add(1*time.Second)))
			return errors.New("<error>")
		}

		stage := Acknowledge(
			backoff.Constant(1 * time.Second),
		)

		err := stage(context.Background(), scope, fail)
		Expect(err).To(MatchError("<error>"))
	})
})
