package eventstore

import (
	"fmt"

	"github.com/dogmatiq/infix/persistence/internal/providertest/common"
	"github.com/dogmatiq/infix/persistence/subsystem/eventstore"
	"github.com/onsi/gomega"
)

// expectEventToEqual asserts that an eventstore.Event equals an expected value.
func expectEventToEqual(
	check, expect *eventstore.Event,
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

// expectEventsToEqual asserts that an eventstore.Event equals an expected value.
func expectEventsToEqual(
	check, expect []*eventstore.Event,
	desc ...interface{},
) {
	gomega.Expect(check).To(gomega.HaveLen(len(expect)))

	for i, ev := range check {
		expectEventToEqual(
			ev, expect[i],
			common.ExpandDescription(
				desc,
				fmt.Sprintf("event at index #%d of slice", i),
			),
		)
	}
}
