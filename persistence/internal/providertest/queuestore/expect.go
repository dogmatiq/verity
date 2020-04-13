package queuestore

import (
	"fmt"

	"github.com/dogmatiq/infix/persistence/internal/providertest/common"
	"github.com/dogmatiq/infix/persistence/subsystem/queuestore"
	"github.com/onsi/gomega"
)

// expectParcelToEqual asserts that an queuestore.Parcel equals an
// expected value, including the revisions.
func expectParcelToEqual(check, expect *queuestore.Parcel, desc ...interface{}) {
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

// expectParcelsToEqual asserts that a slice of queuestore.Parcel equals an
// expected value, including the revisions.
func expectParcelsToEqual(check, expect []*queuestore.Parcel, desc ...interface{}) {
	gomega.Expect(check).To(gomega.HaveLen(len(expect)))

	for i, p := range check {
		expectParcelToEqual(
			p, expect[i],
			common.ExpandDescription(
				desc,
				fmt.Sprintf("parcel at index #%d of slice", i),
			),
		)
	}
}
