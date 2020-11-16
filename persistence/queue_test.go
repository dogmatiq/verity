package persistence_test

import (
	. "github.com/dogmatiq/dogma/fixtures"
	. "github.com/dogmatiq/verity/fixtures"
	. "github.com/dogmatiq/verity/persistence"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("type QueueMessage", func() {
	Describe("func ID()", func() {
		It("returns the ID from the envelope", func() {
			m := QueueMessage{
				Envelope: NewEnvelope("<id>", MessageA1),
			}

			Expect(m.ID()).To(Equal("<id>"))
		})
	})
})
