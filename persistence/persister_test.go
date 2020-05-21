package persistence_test

import (
	"context"
	"errors"

	. "github.com/dogmatiq/dogma/fixtures"
	. "github.com/dogmatiq/infix/fixtures"
	. "github.com/dogmatiq/infix/persistence"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/ginkgo/extensions/table"
	. "github.com/onsi/gomega"
)

var _ = Describe("type Operation (interface)", func() {
	Describe("func AcceptVisitor()", func() {
		DescribeTable(
			"it calls the correct method on the visitor",
			func(op Operation, expect string) {
				var v errorVisitor
				err := op.AcceptVisitor(context.Background(), v)
				Expect(err).To(MatchError(expect))
			},
			Entry("type SaveAggregateMetaData", SaveAggregateMetaData{}, "SaveAggregateMetaData"),
			Entry("type SaveEvent", SaveEvent{}, "SaveEvent"),
			Entry("type SaveQueueMessage", SaveQueueMessage{}, "SaveQueueMessage"),
			Entry("type RemoveQueueMessage", RemoveQueueMessage{}, "RemoveQueueMessage"),
			Entry("type SaveOffset", SaveOffset{}, "SaveOffset"),
		)
	})
})

var _ = Describe("type Batch", func() {
	Describe("func MustValidate()", func() {
		It("panics if the batch contains multiple operations on the same entity", func() {
			env := NewEnvelope("<id>", MessageA1)

			batch := Batch{
				SaveQueueMessage{
					Message: QueueMessage{
						Envelope: env,
					},
				},
				RemoveQueueMessage{
					Message: QueueMessage{
						Envelope: env,
					},
				},
			}

			Expect(func() {
				batch.MustValidate()
			}).To(PanicWith(
				"batch contains multiple operations for the same entity (queue <id>)",
			))
		})

		It("does not panic if the batch contains no operations for the same entity", func() {
			batch := Batch{
				SaveAggregateMetaData{
					MetaData: AggregateMetaData{
						HandlerKey: "<handler-key>",
						InstanceID: "<instance>",
					},
				},
				SaveEvent{
					Envelope: NewEnvelope("<id-1>", MessageA1),
				},
				SaveQueueMessage{
					Message: QueueMessage{
						Envelope: NewEnvelope("<id-1>", MessageA1), // Note, same as SaveEvent, this is allowed and required.
					},
				},
				RemoveQueueMessage{
					Message: QueueMessage{
						Envelope: NewEnvelope("<id-2>", MessageA1),
					},
				},
				SaveOffset{
					ApplicationKey: "<app-key>",
				},
			}

			Expect(func() {
				batch.MustValidate()
			}).ToNot(Panic())
		})
	})

	Describe("func AcceptVisitor()", func() {
		It("visits each operation in the batch", func() {
			batch := Batch{
				SaveAggregateMetaData{
					MetaData: AggregateMetaData{
						HandlerKey: "<handler-key>",
						InstanceID: "<instance>",
					},
				},
				SaveEvent{
					Envelope: NewEnvelope("<id-1>", MessageA1),
				},
				SaveQueueMessage{
					Message: QueueMessage{
						Envelope: NewEnvelope("<id-1>", MessageA1), // Note, same as SaveEvent, this is allowed and required.
					},
				},
				RemoveQueueMessage{
					Message: QueueMessage{
						Envelope: NewEnvelope("<id-2>", MessageA1),
					},
				},
				SaveOffset{
					ApplicationKey: "<app-key>",
				},
			}

			var v visitor
			err := batch.AcceptVisitor(context.Background(), &v)
			Expect(err).ShouldNot(HaveOccurred())
			Expect(v.operations).To(BeEquivalentTo(batch))
		})

		It("returns the error from the visitor", func() {
			batch := Batch{
				SaveAggregateMetaData{
					MetaData: AggregateMetaData{
						HandlerKey: "<handler-key>",
						InstanceID: "<instance>",
					},
				},
			}

			var v errorVisitor
			err := batch.AcceptVisitor(context.Background(), v)
			Expect(err).To(MatchError("SaveAggregateMetaData"))
		})
	})
})

type visitor struct {
	operations []Operation
}

func (v *visitor) VisitSaveAggregateMetaData(_ context.Context, op SaveAggregateMetaData) error {
	v.operations = append(v.operations, op)
	return nil
}

func (v *visitor) VisitSaveEvent(_ context.Context, op SaveEvent) error {
	v.operations = append(v.operations, op)
	return nil
}

func (v *visitor) VisitSaveQueueMessage(_ context.Context, op SaveQueueMessage) error {
	v.operations = append(v.operations, op)
	return nil
}

func (v *visitor) VisitRemoveQueueMessage(_ context.Context, op RemoveQueueMessage) error {
	v.operations = append(v.operations, op)
	return nil
}

func (v *visitor) VisitSaveOffset(_ context.Context, op SaveOffset) error {
	v.operations = append(v.operations, op)
	return nil
}

type errorVisitor struct{}

func (errorVisitor) VisitSaveAggregateMetaData(_ context.Context, op SaveAggregateMetaData) error {
	return errors.New("SaveAggregateMetaData")
}

func (errorVisitor) VisitSaveEvent(_ context.Context, op SaveEvent) error {
	return errors.New("SaveEvent")
}

func (errorVisitor) VisitSaveQueueMessage(_ context.Context, op SaveQueueMessage) error {
	return errors.New("SaveQueueMessage")
}

func (errorVisitor) VisitRemoveQueueMessage(_ context.Context, op RemoveQueueMessage) error {
	return errors.New("RemoveQueueMessage")
}

func (errorVisitor) VisitSaveOffset(_ context.Context, op SaveOffset) error {
	return errors.New("SaveOffset")
}
