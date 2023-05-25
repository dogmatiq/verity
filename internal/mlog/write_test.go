package mlog_test

import (
	"strings"

	. "github.com/dogmatiq/verity/internal/mlog"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func describeTable(
	description string,
	fn func(expected string, ids []IconWithLabel, icons []Icon, text []string),
) bool {
	return DescribeTable(
		description,
		fn,
		Entry(
			"renders a standard log message",
			"= 123  ∵ 456  ⋲ 789  ▼ ↻  <foo> ● <bar>",
			[]IconWithLabel{
				MessageIDIcon.WithLabel("123"),
				CausationIDIcon.WithLabel("456"),
				CorrelationIDIcon.WithLabel("789"),
			},
			[]Icon{
				ConsumeIcon,
				RetryIcon,
			},
			[]string{
				"<foo>",
				"<bar>",
			},
		),
		Entry(
			"renders a hyphen in place of empty labels",
			"= 123  ∵ 456  ⋲ -  ▼    <foo> ● <bar>",
			[]IconWithLabel{
				MessageIDIcon.WithLabel("123"),
				CausationIDIcon.WithLabel("456"),
				CorrelationIDIcon.WithLabel(""),
			},
			[]Icon{
				ConsumeIcon,
				"",
			},
			[]string{
				"<foo>",
				"<bar>",
			},
		),
		Entry(
			"pads empty icons to the same width",
			"= 123  ∵ 456  ⋲ 789  ▼    <foo> ● <bar>",
			[]IconWithLabel{
				MessageIDIcon.WithLabel("123"),
				CausationIDIcon.WithLabel("456"),
				CorrelationIDIcon.WithLabel("789"),
			},
			[]Icon{
				ConsumeIcon,
				"",
			},
			[]string{
				"<foo>",
				"<bar>",
			},
		),

		Entry(
			"skips empty text",
			"= 123  ∵ 456  ⋲ 789  ▼ ↻  <foo> ● <bar>",
			[]IconWithLabel{
				MessageIDIcon.WithLabel("123"),
				CausationIDIcon.WithLabel("456"),
				CorrelationIDIcon.WithLabel("789"),
			},
			[]Icon{
				ConsumeIcon,
				RetryIcon,
			},
			[]string{
				"<foo>",
				"",
				"<bar>",
			},
		),
	)
}

var _ = describeTable(
	"func String()",
	func(expected string, ids []IconWithLabel, icons []Icon, text []string) {
		Expect(
			String(ids, icons, text...),
		).To(Equal(expected))
	},
)

var _ = describeTable(
	"func Write()",
	func(expected string, ids []IconWithLabel, icons []Icon, text []string) {
		w := &strings.Builder{}

		n, err := Write(w, ids, icons, text...)
		Expect(err).ShouldNot(HaveOccurred())
		Expect(n).To(Equal(len(expected)))

		Expect(w.String()).To(Equal(expected))
	},
)
