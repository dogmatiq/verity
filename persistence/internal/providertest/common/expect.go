package common

import (
	"fmt"

	"github.com/golang/protobuf/proto"
	"github.com/onsi/gomega"
)

// ExpectProtoToEqual asserts that a protobuf message equals an expected value.
//
// This is necessary because protocol buffers structs contain internal data that
// may differ for two messages that are equivalent to the user.
func ExpectProtoToEqual(
	check, expect proto.Message,
	desc ...interface{},
) {
	if proto.Equal(check, expect) {
		return
	}

	gomega.Expect(check).To(
		gomega.Equal(expect),
		ExpandDescription(desc, "protocol buffer messages differ"),
	)
}

// ExpandDescription returns a test description with an optional Gomega style
// description preprended.
func ExpandDescription(desc []interface{}, text string) string {
	for _, d := range desc {
		text = fmt.Sprintf("%s: ", d) + text
	}

	return text
}
