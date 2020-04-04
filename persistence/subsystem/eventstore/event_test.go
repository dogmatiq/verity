package eventstore_test

import (
	. "github.com/dogmatiq/dogma/fixtures"
	. "github.com/dogmatiq/infix/fixtures"
	. "github.com/dogmatiq/infix/persistence/subsystem/eventstore"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("type Event", func() {
	Describe("func ID()", func() {
		It("returns the ID from the envelope", func() {
			ev := &Event{
				Envelope: NewEnvelopeProto("<id>", MessageA1),
			}

			Expect(ev.ID()).To(Equal("<id>"))
		})
	})
})
