package eventstore

import (
	"fmt"

	"github.com/dogmatiq/infix/persistence/internal/providertest/common"
	"github.com/dogmatiq/infix/persistence/subsystem/eventstore"
	"github.com/onsi/gomega"
)

// expectParcelToEqual asserts that an eventstore.Parcel equals an expected value.
func expectParcelToEqual(
	check, expect *eventstore.Parcel,
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

// expectParcelsToEqual asserts that a slice of eventstore.Parcel equals an
// expected value.
func expectParcelsToEqual(
	check, expect []*eventstore.Parcel,
	desc ...interface{},
) {
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
