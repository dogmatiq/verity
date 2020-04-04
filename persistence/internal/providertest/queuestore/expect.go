package queuestore

import (
	"fmt"

	"github.com/dogmatiq/infix/persistence/internal/providertest/common"
	"github.com/dogmatiq/infix/persistence/subsystem/queuestore"
	"github.com/onsi/gomega"
)

// expectMessageToEqual asserts that an queuestore.Message equals an
// expected value, including the revisions.
func expectMessageToEqual(check, expect *queuestore.Message, desc ...interface{}) {
	gomega.Expect(check.Revision).To(
		gomega.Equal(expect.Revision),
		common.ExpandDescription(desc, "revision does not match"),
	)

	expectMessageToEqualNoRev(check, expect, desc...)
}

// expectMessageToEqualNoRev asserts that an queuestore.Message equals an expected
// value. It does not compare the revisions.
func expectMessageToEqualNoRev(check, expect *queuestore.Message, desc ...interface{}) {
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

// expectMessagesToEqual asserts that a slice of queuestore.Message equals an
// expected value, including the revisions.
func expectMessagesToEqual(check, expect []*queuestore.Message, desc ...interface{}) {
	gomega.Expect(check).To(gomega.HaveLen(len(expect)))

	for i, ev := range check {
		expectMessageToEqual(
			ev, expect[i],
			common.ExpandDescription(
				desc,
				fmt.Sprintf("message at index #%d of slice", i),
			),
		)
	}
}

// expectMessagesToEqualNoRev asserts that a slice of queuestore.Message equals
// an expected value. It does not compare the revisions.
func expectMessagesToEqualNoRev(check, expect []*queuestore.Message, desc ...interface{}) {
	gomega.Expect(check).To(gomega.HaveLen(len(expect)))

	for i, ev := range check {
		expectMessageToEqualNoRev(
			ev, expect[i],
			common.ExpandDescription(
				desc,
				fmt.Sprintf("message at index #%d of slice", i),
			),
		)
	}
}
