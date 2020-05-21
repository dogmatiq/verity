package persistence_test

import (
	. "github.com/dogmatiq/dogma/fixtures"
	. "github.com/dogmatiq/infix/fixtures"
	. "github.com/dogmatiq/infix/persistence"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("type Event", func() {
	Describe("func ID()", func() {
		It("returns the ID from the envelope", func() {
			ev := &Event{
				Envelope: NewEnvelope("<id>", MessageA1),
			}

			Expect(ev.ID()).To(Equal("<id>"))
		})
	})
})
