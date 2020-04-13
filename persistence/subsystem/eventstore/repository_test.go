package eventstore_test

import (
	"github.com/dogmatiq/infix/draftspecs/envelopespec"
	"github.com/dogmatiq/infix/persistence/subsystem/eventstore"
	. "github.com/dogmatiq/infix/persistence/subsystem/eventstore"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/ginkgo/extensions/table"
	. "github.com/onsi/gomega"
)

var _ = Describe("type Filter", func() {
	Describe("func NewFilter()", func() {
		It("returns a filter containing the given names", func() {
			f := NewFilter("<name-1>", "<name-2>")

			Expect(f).To(Equal(
				Filter{
					"<name-1>": struct{}{},
					"<name-2>": struct{}{},
				},
			))
		})
	})

	Describe("func Add()", func() {
		It("adds a name to the filter", func() {
			f := NewFilter("<name-1>")
			f.Add("<name-2>")

			Expect(f).To(Equal(
				Filter{
					"<name-1>": struct{}{},
					"<name-2>": struct{}{},
				},
			))
		})

		It("initializes the map", func() {
			var f Filter
			f.Add("<name>")

			Expect(f).To(Equal(
				Filter{
					"<name>": struct{}{},
				},
			))
		})
	})

	Describe("func Remove()", func() {
		It("removes a name from the filter", func() {
			f := NewFilter("<name-1>", "<name-2>")
			f.Remove("<name-1>")

			Expect(f).To(Equal(
				Filter{
					"<name-2>": struct{}{},
				},
			))
		})
	})
})

var _ = Describe("type Query", func() {
	Describe("func IsMatch()", func() {
		DescribeTable(
			"it returns true if the event matches",
			func(q Query, p *eventstore.Parcel) {
				Expect(q.IsMatch(p)).To(BeTrue())
			},
			Entry(
				"min-offset (equal)",
				Query{MinOffset: 3},
				&Parcel{Offset: 3},
			),
			Entry(
				"min-offset (greater)",
				Query{MinOffset: 3},
				&Parcel{Offset: 4},
			),
			Entry(
				"type filter",
				Query{
					Filter: NewFilter("<name-1>", "<name-2>"),
				},
				&Parcel{
					Envelope: &envelopespec.Envelope{
						PortableName: "<name-1>",
					},
				},
			),
			Entry(
				"aggregate handler and instance",
				Query{
					AggregateHandlerKey: "<handler>",
					AggregateInstanceID: "<instance>",
				},
				&Parcel{
					Envelope: &envelopespec.Envelope{
						MetaData: &envelopespec.MetaData{
							Source: &envelopespec.Source{
								Handler: &envelopespec.Identity{
									Key: "<handler>",
								},
								InstanceId: "<instance>",
							},
						},
					},
				},
			),
		)

		DescribeTable(
			"it returns false if the event does not match",
			func(q Query, p *eventstore.Parcel) {
				Expect(q.IsMatch(p)).To(BeFalse())
			},
			Entry(
				"min-offset",
				Query{MinOffset: 3},
				&Parcel{Offset: 2},
			),
			Entry(
				"type filter",
				Query{
					Filter: NewFilter("<name-1>", "<name-2>"),
				},
				&Parcel{
					Envelope: &envelopespec.Envelope{
						PortableName: "<different>",
					},
				},
			),
			Entry(
				"aggregate handler and instance (different handler key)",
				Query{
					AggregateHandlerKey: "<handler>",
					AggregateInstanceID: "<instance>",
				},
				&Parcel{
					Envelope: &envelopespec.Envelope{
						MetaData: &envelopespec.MetaData{
							Source: &envelopespec.Source{
								Handler: &envelopespec.Identity{
									Key: "<different>",
								},
								InstanceId: "<instance>",
							},
						},
					},
				},
			),
			Entry(
				"aggregate handler and instance (different instance ID)",
				Query{
					AggregateHandlerKey: "<handler>",
					AggregateInstanceID: "<instance>",
				},
				&Parcel{
					Envelope: &envelopespec.Envelope{
						MetaData: &envelopespec.MetaData{
							Source: &envelopespec.Source{
								Handler: &envelopespec.Identity{
									Key: "<handler>",
								},
								InstanceId: "<different>",
							},
						},
					},
				},
			),
		)
	})
})
