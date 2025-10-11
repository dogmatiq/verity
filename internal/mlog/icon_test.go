package mlog_test

import (
	"github.com/dogmatiq/configkit"
	"github.com/dogmatiq/enginekit/protobuf/uuidpb"
	. "github.com/dogmatiq/verity/internal/mlog"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("type Icon", func() {
	Describe("func String()", func() {
		It("returns the icon string", func() {
			Expect(
				TransactionIDIcon.String(),
			).To(Equal("⨀"))
		})
	})

	Describe("func WithLabel()", func() {
		It("returns the icon and label", func() {
			Expect(
				TransactionIDIcon.WithLabel("<foo>").String(),
			).To(Equal("⨀ <foo>"))
		})
	})

	Describe("func WithID()", func() {
		It("returns the icon and label", func() {
			Expect(
				TransactionIDIcon.WithID(uuidpb.MustParse("47d10297-8192-40c4-aa77-ad63e7d4a8cb")).String(),
			).To(Equal("⨀ 47d10297"))
		})
	})
})

var _ = Describe("func HandlerTypeIcon()", func() {
	It("returns the expected icon", func() {
		Expect(HandlerTypeIcon(configkit.AggregateHandlerType)).To(Equal(AggregateIcon))
		Expect(HandlerTypeIcon(configkit.ProcessHandlerType)).To(Equal(ProcessIcon))
		Expect(HandlerTypeIcon(configkit.IntegrationHandlerType)).To(Equal(IntegrationIcon))
		Expect(HandlerTypeIcon(configkit.ProjectionHandlerType)).To(Equal(ProjectionIcon))
	})
})
