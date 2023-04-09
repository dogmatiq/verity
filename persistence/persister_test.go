package persistence_test

import (
	"context"
	"errors"

	. "github.com/dogmatiq/dogma/fixtures"
	. "github.com/dogmatiq/verity/fixtures"
	. "github.com/dogmatiq/verity/persistence"
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
			Entry("type SaveProcessInstance", SaveProcessInstance{}, "SaveProcessInstance"),
			Entry("type RemoveProcessInstance", RemoveProcessInstance{}, "RemoveProcessInstance"),
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
						HandlerKey: "e4ff048e-79f7-45e2-9f02-3b10d17614c6",
						InstanceID: "<instance>",
					},
				},
				SaveEvent{
					Envelope: NewEnvelope("<id-1>", MessageA1),
				},
				SaveProcessInstance{
					Instance: ProcessInstance{
						HandlerKey: "2ae0b937-e806-4e70-9b23-f36298f68973",
						InstanceID: "<instance>", // note: same instance ID as aggregate, this is allowed
					},
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
				SaveProcessInstance{
					Instance: ProcessInstance{
						HandlerKey: "2ae0b937-e806-4e70-9b23-f36298f68973",
						InstanceID: "<instance-a>",
					},
				},
				RemoveProcessInstance{
					Instance: ProcessInstance{
						HandlerKey: "2ae0b937-e806-4e70-9b23-f36298f68973",
						InstanceID: "<instance-b>",
					},
				},
				SaveOffset{
					ApplicationKey: DefaultAppKey,
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
						HandlerKey: DefaultHandlerKey,
						InstanceID: "<instance>",
					},
				},
				SaveEvent{
					Envelope: NewEnvelope("<id-1>", MessageA1),
				},
				SaveProcessInstance{
					Instance: ProcessInstance{
						HandlerKey: "2ae0b937-e806-4e70-9b23-f36298f68973",
						InstanceID: "<instance>", // note: same instance ID as aggregate, this is allowed
					},
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
					ApplicationKey: DefaultAppKey,
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
						HandlerKey: DefaultHandlerKey,
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

func (v *visitor) VisitSaveProcessInstance(_ context.Context, op SaveProcessInstance) error {
	v.operations = append(v.operations, op)
	return nil
}

func (v *visitor) VisitRemoveProcessInstance(_ context.Context, op RemoveProcessInstance) error {
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

func (errorVisitor) VisitSaveProcessInstance(_ context.Context, op SaveProcessInstance) error {
	return errors.New("SaveProcessInstance")
}

func (errorVisitor) VisitRemoveProcessInstance(_ context.Context, op RemoveProcessInstance) error {
	return errors.New("RemoveProcessInstance")
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
