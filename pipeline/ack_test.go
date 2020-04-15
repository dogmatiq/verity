package pipeline_test

import (
	"context"
	"errors"
	"time"

	"github.com/dogmatiq/dodeca/logging"
	. "github.com/dogmatiq/dogma/fixtures"
	. "github.com/dogmatiq/infix/fixtures"
	. "github.com/dogmatiq/infix/pipeline"
	"github.com/dogmatiq/linger/backoff"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("func Acknowledge()", func() {
	var (
		sess   *SessionStub
		scope  *Scope
		logger *logging.BufferedLogger
		ack    Stage
	)

	BeforeEach(func() {
		scope, sess, _ = NewPipelineScope(
			NewEnvelope("<consume>", MessageC1),
			nil,
		)

		logger = scope.Logger.(*logging.BufferedLogger)

		ack = Acknowledge(
			backoff.Constant(1 * time.Second),
		)
	})

	Context("when the next stage succeeds", func() {
		next := pass

		It("acknowledges the session", func() {
			called := false
			sess.AckFunc = func(context.Context) error {
				called = true
				return nil
			}

			err := ack(context.Background(), scope, next)
			Expect(err).ShouldNot(HaveOccurred())
			Expect(called).To(BeTrue())
		})

		It("logs about consuming", func() {
			err := ack(context.Background(), scope, next)
			Expect(err).ShouldNot(HaveOccurred())
			Expect(logger.Messages()).To(ContainElement(
				logging.BufferedLogMessage{
					Message: "= <consume>  ∵ <cause>  ⋲ <correlation>  ▼    MessageC ● {C1}",
				},
			))
		})

		It("returns an error if Ack() fails", func() {
			sess.AckFunc = func(context.Context) error {
				return errors.New("<error>")
			}

			err := ack(context.Background(), scope, next)
			Expect(err).To(MatchError("<error>"))
		})
	})

	Context("when the next stage fails", func() {
		next := fail

		It("negatively acknowledges the session", func() {
			called := false
			sess.NackFunc = func(_ context.Context, n time.Time) error {
				called = true
				Expect(n).To(BeTemporally("~", time.Now().Add(1*time.Second)))
				return nil
			}

			err := ack(context.Background(), scope, next)
			Expect(err).ShouldNot(HaveOccurred())
			Expect(called).To(BeTrue())
		})

		It("logs about consuming", func() {
			err := ack(context.Background(), scope, next)
			Expect(err).ShouldNot(HaveOccurred())
			Expect(logger.Messages()).To(ContainElement(
				logging.BufferedLogMessage{
					Message: "= <consume>  ∵ <cause>  ⋲ <correlation>  ▼    MessageC ● {C1}",
				},
			))
		})

		It("logs about negative acknowledgement", func() {
			err := ack(context.Background(), scope, next)
			Expect(err).ShouldNot(HaveOccurred())
			Expect(logger.Messages()).To(ContainElement(
				logging.BufferedLogMessage{
					Message: "= <consume>  ∵ <cause>  ⋲ <correlation>  ▽ ✖  MessageC ● <failed> ● next retry in 1s",
				},
			))
		})

		It("returns an error if Nack() fails", func() {
			sess.NackFunc = func(context.Context, time.Time) error {
				return errors.New("<error>")
			}

			err := ack(context.Background(), scope, next)
			Expect(err).To(MatchError("<error>"))
		})

		It("uses the default backoff strategy", func() {
			now := time.Now()
			sess.NackFunc = func(_ context.Context, n time.Time) error {
				Expect(n).To(BeTemporally(">=", now))
				return nil
			}

			ack = Acknowledge(nil)
			err := ack(context.Background(), scope, next)
			Expect(err).ShouldNot(HaveOccurred())
		})
	})
})
