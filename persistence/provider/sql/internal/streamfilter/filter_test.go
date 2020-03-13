package streamfilter_test

import (
	. "github.com/dogmatiq/configkit/fixtures"
	"github.com/dogmatiq/configkit/message"
	. "github.com/dogmatiq/infix/persistence/provider/sql/internal/streamfilter"
	. "github.com/dogmatiq/marshalkit/fixtures"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/ginkgo/extensions/table"
	. "github.com/onsi/gomega"
)

var _ = Describe("func Hash()", func() {
	It("returns a SHA-256 has of the message names", func() {
		hash, _ := Hash(
			Marshaler,
			message.NewTypeSet(
				MessageCType,
				MessageBType,
				MessageAType,
			),
		)

		Expect(hash).To(Equal(
			[]byte("ab687816a9007d3789daab07726757cb32b563fd26e940deea54e2ff596d941f"),
		))
	})
	It("returns the marshaled names, in order", func() {
		_, names := Hash(
			Marshaler,
			message.NewTypeSet(
				MessageCType,
				MessageBType,
				MessageAType,
			),
		)

		Expect(names).To(Equal(
			[]string{
				"MessageA",
				"MessageB",
				"MessageC",
			},
		))
	})
})

var _ = Describe("func CompareNames()", func() {
	It("returns true if the slices contain the same elements in the same order", func() {
		ok := CompareNames(
			[]string{"a", "b", "c"},
			[]string{"a", "b", "c"},
		)
		Expect(ok).To(BeTrue())
	})

	DescribeTable(
		"it returns false if the slices are not equal",
		func(a, b []string) {
			ok := CompareNames(a, b)
			Expect(ok).To(BeFalse())
		},
		Entry(
			"different order",
			[]string{"a", "b", "c"},
			[]string{"c", "b", "a"},
		),
		Entry(
			"different elements",
			[]string{"a", "b", "c"},
			[]string{"a", "b", "XXX"},
		),
		Entry(
			"different lengths",
			[]string{"a", "b", "c"},
			[]string{"a", "b"},
		),
	)
})
