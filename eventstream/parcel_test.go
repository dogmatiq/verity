package eventstream_test

import (
	. "github.com/dogmatiq/dogma/fixtures"
	. "github.com/dogmatiq/infix/eventstream"
	. "github.com/dogmatiq/infix/fixtures"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("type Parcel", func() {
	Describe("func ID()", func() {
		It("returns the ID from the envelope", func() {
			p := &Parcel{
				Envelope: NewEnvelope("<id>", MessageA1),
			}

			Expect(p.ID()).To(Equal("<id>"))
		})
	})
})