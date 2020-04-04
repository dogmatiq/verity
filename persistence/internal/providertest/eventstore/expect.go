package eventstore

import (
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
