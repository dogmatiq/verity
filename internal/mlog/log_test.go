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

var _ = Describe("func LogConsume()", func() {
	It("logs in the correct format", func() {
		logger := &logging.BufferedLogger{}

		LogConsume(
			logger,
			NewEnvelope("<id>", MessageA1),
			0,
		)

		Expect(logger.Messages()).To(ContainElement(
			logging.BufferedLogMessage{
				Message: "= <id>  ∵ <cause>  ⋲ <correlation>  ▼    MessageA ● {A1}",
			},
		))
	})

	It("shows a retry icon if the failure count is non-zero", func() {
		logger := &logging.BufferedLogger{}

		LogConsume(
			logger,
			NewEnvelope("<id>", MessageA1),
			1,
		)

		Expect(logger.Messages()).To(ContainElement(
			logging.BufferedLogMessage{
				Message: "= <id>  ∵ <cause>  ⋲ <correlation>  ▼ ↻  MessageA ● {A1}",
			},
		))
	})
})

var _ = Describe("func LogProduce()", func() {
	It("logs in the correct format", func() {
		logger := &logging.BufferedLogger{}

		LogProduce(
			logger,
			NewEnvelope("<id>", MessageA1),
		)

		Expect(logger.Messages()).To(ContainElement(
			logging.BufferedLogMessage{
				Message: "= <id>  ∵ <cause>  ⋲ <correlation>  ▲    MessageA ● {A1}",
			},
		))
	})
})

var _ = Describe("func LogNack()", func() {
	It("logs in the correct format", func() {
		logger := &logging.BufferedLogger{}

		LogNack(
			logger,
			NewEnvelope("<id>", MessageA1),
			errors.New("<error>"),
			5*time.Second,
		)

		Expect(logger.Messages()).To(ContainElement(
			logging.BufferedLogMessage{
				Message: "= <id>  ∵ <cause>  ⋲ <correlation>  ▽ ✖  MessageA ● <error> ● next retry in 5s",
			},
		))
	})
})

var _ = Describe("func LogFromScope()", func() {
	It("logs in the correct format", func() {
		logger := &logging.BufferedLogger{}

		LogFromScope(
			logger,
			NewEnvelope("<id>", MessageA1),
			"format %s",
			[]interface{}{"<value>"},
		)

		Expect(logger.Messages()).To(ContainElement(
			logging.BufferedLogMessage{
				Message: "= <id>  ∵ <cause>  ⋲ <correlation>  ▼    MessageA ● format <value>",
			},
		))
	})
})
