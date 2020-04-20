package projection

import (
	"github.com/dogmatiq/dodeca/logging"
	. "github.com/dogmatiq/dogma/fixtures"
	. "github.com/dogmatiq/infix/fixtures"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("type scope", func() {
	var (
		logger *logging.BufferedLogger
		sc     *scope
	)

	BeforeEach(func() {
		logger = &logging.BufferedLogger{}

		sc = &scope{
			cause:  NewParcel("<consume>", MessageC1),
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
					Message: "= <consume>  ∵ <cause>  ⋲ <correlation>  ▼    MessageC ● format <value>",
				},
			))
		})
	})
})
