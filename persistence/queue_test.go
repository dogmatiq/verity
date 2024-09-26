package persistence_test

import (
	. "github.com/dogmatiq/enginekit/enginetest/stubs"
	. "github.com/dogmatiq/verity/fixtures"
	. "github.com/dogmatiq/verity/persistence"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("type QueueMessage", func() {
	Describe("func ID()", func() {
		It("returns the ID from the envelope", func() {
			m := QueueMessage{
				Envelope: NewEnvelope("<id>", CommandA1),
			}

			Expect(m.ID()).To(Equal("<id>"))
		})
	})
})
