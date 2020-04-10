package pipeline_test

import (
	"context"
	"errors"
	"time"

	. "github.com/dogmatiq/dogma/fixtures"
	"github.com/dogmatiq/infix/envelope"
	. "github.com/dogmatiq/infix/fixtures"
	. "github.com/dogmatiq/infix/pipeline"
	"github.com/dogmatiq/linger/backoff"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/ginkgo/extensions/table"
	. "github.com/onsi/gomega"
)

var _ = Describe("func Acknowledge()", func() {
	var (
		session *SessionStub
		scope   *Scope
	)

	BeforeEach(func() {
		env := NewEnvelope("<id>", MessageA1)

		session = &SessionStub{
			EnvelopeFunc: func() (*envelope.Envelope, error) {
				return env, nil
			},
		}

		scope = &Scope{
			Session: session,
		}
	})

	DescribeTable(
		"it acknowledges the session if the next stage succeeds",
		func(failureCount int) {
			session.FailureCountFunc = func() uint {
				return uint(failureCount)
			}

			session.AckFunc = func(context.Context) error {
				return errors.New("<error>")
			}

			stage := Acknowledge(nil)

			err := stage(context.Background(), scope, pass)
			Expect(err).To(MatchError("<error>"))
		},
		Entry("first attempt", 0),
		Entry("subsequent attempt", 123),
	)

	DescribeTable(
		"it negatively acknowledges the session if the next stage fails",
		func(failureCount int) {
			session.FailureCountFunc = func() uint {
				return uint(failureCount)
			}

			session.NackFunc = func(_ context.Context, n time.Time) error {
				Expect(n).To(BeTemporally("~", time.Now().Add(1*time.Second)))
				return errors.New("<error>")
			}

			stage := Acknowledge(
				backoff.Constant(1 * time.Second),
			)

			err := stage(context.Background(), scope, fail)
			Expect(err).To(MatchError("<error>"))
		},
		Entry("first attempt", 0),
		Entry("subsequent attempt", 123),
	)

	It("negatively acknowledges the session if envelope can not be unpacked", func() {
		session.EnvelopeFunc = func() (*envelope.Envelope, error) {
			return nil, errors.New("<error>")
		}

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
