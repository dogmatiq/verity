package queuestore

import (
	"fmt"

	"github.com/dogmatiq/infix/persistence/internal/providertest/common"
	"github.com/dogmatiq/infix/persistence/subsystem/queuestore"
	"github.com/onsi/gomega"
)

// expectItemToEqual asserts that an queuestore.Item equals an
// expected value, including the revisions.
//
// TODO: https://github.com/dogmatiq/infix/issues/151
func expectItemToEqual(check, expect *queuestore.Item, desc ...interface{}) {
	gomega.Expect(check.Revision).To(
		gomega.Equal(expect.Revision),
		common.ExpandDescription(desc, "revision does not match"),
	)

	gomega.Expect(check.FailureCount).To(
		gomega.Equal(expect.FailureCount),
		common.ExpandDescription(desc, "failure count time does not match"),
	)

	gomega.Expect(check.NextAttemptAt).To(
		gomega.BeTemporally("~", expect.NextAttemptAt),
		common.ExpandDescription(desc, "next-attempt time does not match"),
	)

	common.ExpectProtoToEqual(
		check.Envelope,
		expect.Envelope,
		common.ExpandDescription(desc, "message envelope does not match"),
	)
}

// expectItemsToEqual asserts that a slice of queuestore.Item equals an
// expected value, including the revisions.
func expectItemsToEqual(check, expect []*queuestore.Item, desc ...interface{}) {
	gomega.Expect(check).To(gomega.HaveLen(len(expect)))

	for i, it := range check {
		expectItemToEqual(
			it, expect[i],
			common.ExpandDescription(
				desc,
				fmt.Sprintf("item at index #%d of slice", i),
			),
		)
	}
}
