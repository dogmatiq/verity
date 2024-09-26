package eventstream_test

import (
	. "github.com/dogmatiq/enginekit/enginetest/stubs"
	. "github.com/dogmatiq/verity/eventstream"
	. "github.com/dogmatiq/verity/fixtures"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("type Event", func() {
	Describe("func ID()", func() {
		It("returns the ID from the parcel", func() {
			ev := Event{
				Parcel: NewParcel("<id>", EventA1),
			}

			Expect(ev.ID()).To(Equal("<id>"))
		})
	})
})
