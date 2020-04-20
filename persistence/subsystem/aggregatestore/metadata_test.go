package aggregatestore_test

import (
	. "github.com/dogmatiq/infix/persistence/subsystem/aggregatestore"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("type MetaData", func() {
	Describe("func InstanceExists()", func() {
		It("returns true if the offset range is not empty", func() {
			md := &MetaData{
				BeginOffset: 1,
				EndOffset:   2,
			}

			Expect(md.InstanceExists()).To(BeTrue())
		})

		It("returns false if the offset range is not empty", func() {
			md := &MetaData{
				BeginOffset: 1,
				EndOffset:   1,
			}

			Expect(md.InstanceExists()).To(BeFalse())
		})
	})

	Describe("func MarkInstanceDestroyed()", func() {
		It("moves the beginning of the range to the end", func() {
			md := &MetaData{
				BeginOffset: 1,
				EndOffset:   2,
			}

			md.MarkInstanceDestroyed()

			Expect(md).To(Equal(
				&MetaData{
					BeginOffset: 2,
					EndOffset:   2,
				},
			))
		})
	})

	Describe("func SetLastRecordedOffset()", func() {
		It("expands the range to include the new offset", func() {
			md := &MetaData{
				BeginOffset: 1,
				EndOffset:   2,
			}

			md.SetLastRecordedOffset(123)

			Expect(md).To(Equal(
				&MetaData{
					BeginOffset: 1,
					EndOffset:   124,
				},
			))
		})
	})
})
