package eventstore

import (
	"fmt"

	"github.com/dogmatiq/infix/persistence/internal/providertest/common"
	"github.com/dogmatiq/infix/persistence/subsystem/eventstore"
	"github.com/onsi/gomega"
)

// expectItemToEqual asserts that an eventstore.Item equals an expected value.
//
// TODO: https://github.com/dogmatiq/infix/issues/151
func expectItemToEqual(
	check, expect *eventstore.Item,
	desc ...interface{},
) {
	gomega.Expect(check.Offset).To(
		gomega.Equal(expect.Offset),
		common.ExpandDescription(desc, "offset does not match"),
	)

	common.ExpectProtoToEqual(
		check.Envelope,
		expect.Envelope,
		common.ExpandDescription(desc, "message envelope does not match"),
	)
}

// expectItemsToEqual asserts that a slice of eventstore.Item equals an
// expected value.
func expectItemsToEqual(
	check, expect []*eventstore.Item,
	desc ...interface{},
) {
	gomega.Expect(check).To(gomega.HaveLen(len(expect)))

	for i, item := range check {
		expectItemToEqual(
			item, expect[i],
			common.ExpandDescription(
				desc,
				fmt.Sprintf("item at index #%d of slice", i),
			),
		)
	}
}
