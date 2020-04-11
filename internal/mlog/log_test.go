package mlog_test

import (
	"errors"
	"time"

	"github.com/dogmatiq/dodeca/logging"
	. "github.com/dogmatiq/dogma/fixtures"
	. "github.com/dogmatiq/infix/fixtures"
	. "github.com/dogmatiq/infix/internal/mlog"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("func LogSuccess()", func() {
	It("logs in the correct format", func() {
		logger := &logging.BufferedLogger{}

		LogSuccess(
			logger,
			NewEnvelope("<id>", MessageA1),
			0,
		)

		Expect(logger.Messages()).To(ContainElement(
			logging.BufferedLogMessage{
				Message: "= <id>  ∵ <cause>  ⋲ <correlation>  ▼    fixtures.MessageA ● {A1}",
			},
		))
	})

	It("shows a retry icon if the failure count is non-zero", func() {
		logger := &logging.BufferedLogger{}

		LogSuccess(
			logger,
			NewEnvelope("<id>", MessageA1),
			1,
		)

		Expect(logger.Messages()).To(ContainElement(
			logging.BufferedLogMessage{
				Message: "= <id>  ∵ <cause>  ⋲ <correlation>  ▼ ↻  fixtures.MessageA ● {A1}",
			},
		))
	})
})

var _ = Describe("func LogFailure()", func() {
	It("logs in the correct format", func() {
		logger := &logging.BufferedLogger{}

		LogFailure(
			logger,
			NewEnvelope("<id>", MessageA1),
			errors.New("<error>"),
			5*time.Second,
		)

		Expect(logger.Messages()).To(ContainElement(
			logging.BufferedLogMessage{
				Message: "= <id>  ∵ <cause>  ⋲ <correlation>  ▽ ✖  fixtures.MessageA ● <error> ● next retry in 5s ● {A1}",
			},
		))
	})
})

var _ = Describe("func LogFailureWithoutEnvelope()", func() {
	It("logs in the correct format", func() {
		logger := &logging.BufferedLogger{}

		LogFailureWithoutEnvelope(
			logger,
			"<id>",
			errors.New("<error>"),
			5*time.Second,
		)

		Expect(logger.Messages()).To(ContainElement(
			logging.BufferedLogMessage{
				Message: "= <id>  ∵ -  ⋲ -  ▽ ✖  <error> ● next retry in 5s",
			},
		))
	})
})
