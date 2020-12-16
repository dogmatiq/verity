package boltpersistence

import (
	"errors"

	"github.com/dogmatiq/verity/internal/x/bboltx"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("func unmarshalUint64()", func() {
	It("panics if the byte-slice is the wrong length", func() {
		Expect(func() {
			unmarshalUint64(make([]byte, 3))
		}).To(PanicWith(
			bboltx.PanicSentinel{
				Cause: errors.New("data is corrupt, expected 8 bytes, got 3"),
			},
		))
	})
})
