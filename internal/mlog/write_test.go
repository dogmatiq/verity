package mlog_test

import (
	"strings"

	. "github.com/dogmatiq/infix/internal/mlog"
	. "github.com/onsi/ginkgo/extensions/table"
	. "github.com/onsi/gomega"
)

var entries = []TableEntry{
	Entry(
		"renders a standard log message",
		"= 123  ∵ 456  ⋲ 789  ▼ ↻  <foo> ● <bar>",
		[]IconWithLabel{
			MessageIDIcon.WithLabel("123"),
			CausationIDIcon.WithLabel("456"),
			CorrelationIDIcon.WithLabel("789"),
		},
		[]Icon{
			InboundIcon,
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
			InboundIcon,
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
			InboundIcon,
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
			InboundIcon,
			RetryIcon,
		},
		[]string{
			"<foo>",
			"",
			"<bar>",
		},
	),
}

var _ = DescribeTable(
	"func String()",
	func(expected string, ids []IconWithLabel, icons []Icon, text []string) {
		Expect(
			String(ids, icons, text...),
		).To(Equal(expected))
	},
	entries...,
)

var _ = DescribeTable(
	"func Write()",
	func(expected string, ids []IconWithLabel, icons []Icon, text []string) {
		w := &strings.Builder{}

		n, err := Write(w, ids, icons, text...)
		Expect(err).ShouldNot(HaveOccurred())
		Expect(n).To(Equal(len(expected)))

		Expect(w.String()).To(Equal(expected))
	},
	entries...,
)
