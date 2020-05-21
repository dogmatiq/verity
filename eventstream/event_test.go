package eventstream_test

import (
	. "github.com/dogmatiq/dogma/fixtures"
	. "github.com/dogmatiq/infix/eventstream"
	. "github.com/dogmatiq/infix/fixtures"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("type Event", func() {
	Describe("func ID()", func() {
		It("returns the ID from the parcel", func() {
			ev := Event{
				Parcel: NewParcel("<id>", MessageA1),
			}

			Expect(ev.ID()).To(Equal("<id>"))
		})
	})
})
