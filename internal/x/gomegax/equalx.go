package gomegax

import (
	"github.com/google/go-cmp/cmp"
	"github.com/onsi/gomega/format"
	"github.com/onsi/gomega/types"
	"google.golang.org/protobuf/testing/protocmp"
)

// EqualX is a more powerful and safer alternative to gomega.Equal() for
// comparing whether two values are semantically equal.
func EqualX(expected interface{}, options ...cmp.Option) types.GomegaMatcher {
	if len(options) == 0 {
		options = append(options, protocmp.Transform())
	}

	return &equalMatcher{
		expected: expected,
		options:  options,
	}
}

type equalMatcher struct {
	expected interface{}
	options  cmp.Options
}

func (matcher *equalMatcher) Match(actual interface{}) (success bool, err error) {
	return cmp.Equal(actual, matcher.expected, matcher.options), nil
}

func (matcher *equalMatcher) FailureMessage(actual interface{}) (message string) {
	actualString, actualOK := actual.(string)
	expectedString, expectedOK := matcher.expected.(string)
	if actualOK && expectedOK {
		return format.MessageWithDiff(actualString, "to equal", expectedString)
	}

	diff := cmp.Diff(actual, matcher.expected, matcher.options)
	return format.Message(actual, "to equal", matcher.expected) +
		"\n\nDiff:\n" + format.IndentString(diff, 1)
}

func (matcher *equalMatcher) NegatedFailureMessage(actual interface{}) (message string) {
	diff := cmp.Diff(actual, matcher.expected, matcher.options)
	return format.Message(actual, "not to equal", matcher.expected) +
		"\n\nDiff:\n" + format.IndentString(diff, 1)
}
