package resource_test

import (
	. "github.com/dogmatiq/verity/handler/projection/resource"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("func FromApplicationKey()", func() {
	It("returns the application key as a byte-slice", func() {
		r := FromApplicationKey("<key>")
		Expect(r).To(Equal([]byte("<key>")))
	})
})

var _ = Describe("func MarshalOffset()", func() {
	It("returns an empty slice for the zero offset", func() {
		v := MarshalOffset(0)
		Expect(v).To(BeEmpty())
	})

	It("returns the previous offset as a big-endian uint64", func() {
		v := MarshalOffset(0x0102030405060708)

		Expect(v).To(Equal(
			[]byte{
				0x01,
				0x02,
				0x03,
				0x04,
				0x05,
				0x06,
				0x07,
				0x07, // v - 1
			},
		))
	})
})

var _ = Describe("func MarshalOffsetInto()", func() {
	buf := make([]byte, 10) // longer than needed

	It("returns an empty slice for the zero offset", func() {
		v := MarshalOffsetInto(buf, 0)
		Expect(v).To(BeEmpty())
	})

	It("returns the previous offset as a big-endian uint64", func() {
		v := MarshalOffsetInto(buf, 0x0102030405060708)

		Expect(buf[:8]).To(Equal(v))
		Expect(v).To(Equal(
			[]byte{
				0x01,
				0x02,
				0x03,
				0x04,
				0x05,
				0x06,
				0x07,
				0x07, // v - 1
			},
		))
	})
})

var _ = Describe("func UnmarshalOffsetInto()", func() {
	It("returns zero if the buffer is empty", func() {
		o, err := UnmarshalOffset(nil)
		Expect(err).ShouldNot(HaveOccurred())
		Expect(o).To(BeNumerically("==", 0))

		o, err = UnmarshalOffset([]byte{})
		Expect(err).ShouldNot(HaveOccurred())
		Expect(o).To(BeNumerically("==", 0))
	})

	It("returns the next offset", func() {
		o, err := UnmarshalOffset([]byte{
			0x01,
			0x02,
			0x03,
			0x04,
			0x05,
			0x06,
			0x07,
			0x07, // o - 1
		})
		Expect(err).ShouldNot(HaveOccurred())
		Expect(o).To(BeNumerically("==", 0x0102030405060708))
	})

	It("returns an error if the byte-slice is an unexpected length", func() {
		_, err := UnmarshalOffset([]byte{0})
		Expect(err).To(MatchError("version is 1 byte(s), expected 0 or 8"))
	})
})
