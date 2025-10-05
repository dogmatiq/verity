package projection

import (
	"time"

	//revive:disable:dot-imports
	"github.com/dogmatiq/dodeca/logging"
	. "github.com/dogmatiq/enginekit/enginetest/stubs"
	. "github.com/dogmatiq/verity/fixtures"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("type eventScope", func() {
	var (
		logger *logging.BufferedLogger
		sc     *eventScope
	)

	BeforeEach(func() {
		logger = &logging.BufferedLogger{}

		sc = &eventScope{
			cause:  NewParcel("<consume>", EventE1),
			logger: logger,
		}
	})

	Describe("func RecordedAt()", func() {
		It("returns the created-at time of the cause", func() {
			Expect(sc.RecordedAt()).To(
				BeTemporally("==", sc.cause.CreatedAt),
			)
		})
	})

	Describe("func Log()", func() {
		It("logs using the standard format", func() {
			sc.Log("format %s", "<value>")

			Expect(logger.Messages()).To(ContainElement(
				logging.BufferedLogMessage{
					Message: "= <consume>  ∵ <cause>  ⋲ <correlation>  ▼    EventStub[TypeE] ● format <value>",
				},
			))
		})
	})
})

var _ = Describe("type compactScope", func() {
	var (
		logger *logging.BufferedLogger
		sc     *compactScope
	)

	BeforeEach(func() {
		logger = &logging.BufferedLogger{}

		sc = &compactScope{
			logger: logger,
		}
	})

	Describe("func Log()", func() {
		It("logs a message", func() {
			sc.Log("format %s", "<value>")

			Expect(logger.Messages()).To(ContainElement(
				logging.BufferedLogMessage{
					Message: "format <value>",
				},
			))
		})
	})

	Describe("func Now()", func() {
		It("returns the current time", func() {
			t := sc.Now()

			Expect(t).To(BeTemporally("~", time.Now()))
		})
	})
})
