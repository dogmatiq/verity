package gomegax

import (
	"fmt"
	"reflect"

	"github.com/onsi/gomega"
	"github.com/onsi/gomega/format"
	"github.com/onsi/gomega/types"
)

// PanicWith is a matcher that expects a specific panic value.
func PanicWith(v interface{}) types.GomegaMatcher {
	m, ok := v.(types.GomegaMatcher)
	if !ok {
		m = gomega.Equal(v)
	}

	return &panicWithMatcher{Matcher: m}
}

type panicWithMatcher struct {
	Matcher  types.GomegaMatcher
	panicked bool
	matched  bool
	object   interface{}
}

func (m *panicWithMatcher) Match(actual interface{}) (bool, error) {
	if actual == nil {
		return false, fmt.Errorf("PanicWith expects a non-nil actual")
	}

	actualType := reflect.TypeOf(actual)
	if actualType.Kind() != reflect.Func {
		return false, fmt.Errorf("PanicWith expects a function.  Got:\n%s", format.Object(actual, 1))
	}
	if !(actualType.NumIn() == 0 && actualType.NumOut() == 0) {
		return false, fmt.Errorf("PanicWith expects a function with no arguments and no return value.  Got:\n%s", format.Object(actual, 1))
	}

	func() {
		defer func() {
			if e := recover(); e != nil {
				m.object = e
				m.panicked = true
			}
		}()

		reflect.ValueOf(actual).Call([]reflect.Value{})
	}()

	if !m.panicked {
		return false, nil
	}

	var err error
	m.matched, err = m.Matcher.Match(m.object)

	return m.matched, err
}

func (m *panicWithMatcher) FailureMessage(actual interface{}) (message string) {
	if m.panicked {
		return m.Matcher.FailureMessage(m.object)
	}

	return format.Message(actual, "to panic")
}

func (m *panicWithMatcher) NegatedFailureMessage(actual interface{}) (message string) {
	return m.Matcher.NegatedFailureMessage(m.object)
}
