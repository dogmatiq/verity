package mlog_test

import (
	. "github.com/dogmatiq/verity/internal/mlog"
	"github.com/google/uuid"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("func FormatID()", func() {
	It("returns the first 8 characters of a UUID", func() {
		id := uuid.NewString()
		f := FormatID(id)

		Expect(f).To(Equal(id[:8]))
	})

	It("returns the entire string if it is not a UUID", func() {
		id := "<this is the id>"
		f := FormatID(id)

		Expect(f).To(Equal(id))
	})
})
