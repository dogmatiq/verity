package aggregatestore_test

import (
	. "github.com/dogmatiq/infix/persistence/subsystem/aggregatestore"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("type MetaData", func() {
	Describe("func MarkInstanceDestroyed()", func() {
		It("updates the meta-data state to represent a destroyed instance", func() {
			md := &MetaData{
				InstanceExists: true,
				BeginOffset:    1,
				EndOffset:      2,
			}

			md.MarkInstanceDestroyed("<id>")

			Expect(md).To(Equal(
				&MetaData{
					InstanceExists:  false,
					LastDestroyedBy: "<id>",
					BeginOffset:     2,
					EndOffset:       2,
				},
			))
		})
	})

	Describe("func SetLastRecordedOffset()", func() {
		It("updates the meta-data state to represent an instance that exists", func() {
			md := &MetaData{
				InstanceExists:  false,
				LastDestroyedBy: "<id>",
				BeginOffset:     1,
				EndOffset:       2,
			}

			md.SetLastRecordedOffset(123)

			Expect(md).To(Equal(
				&MetaData{
					InstanceExists:  true,
					LastDestroyedBy: "<id>", // unchanged
					BeginOffset:     1,
					EndOffset:       124,
				},
			))
		})
	})
})
