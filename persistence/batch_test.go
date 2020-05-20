package persistence_test

import (
	"context"

	. "github.com/dogmatiq/dogma/fixtures"
	. "github.com/dogmatiq/infix/fixtures"
	. "github.com/dogmatiq/infix/persistence"
	"github.com/dogmatiq/infix/persistence/subsystem/queuestore"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("type Batch", func() {
	Describe("func MustValidate()", func() {
		It("panics if the batch contains multiple operations on the same entity", func() {
			env := NewEnvelope("<id>", MessageA1)

			batch := Batch{
				SaveQueueItem{
					Item: queuestore.Item{
						Envelope: env,
					},
				},
				RemoveQueueItem{
					Item: queuestore.Item{
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
				SaveQueueItem{
					Item: queuestore.Item{
						Envelope: NewEnvelope("<id-1>", MessageA1), // Note, same as SaveEvent, this is allowed and required.
					},
				},
				RemoveQueueItem{
					Item: queuestore.Item{
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
				SaveQueueItem{
					Item: queuestore.Item{
						Envelope: NewEnvelope("<id-1>", MessageA1), // Note, same as SaveEvent, this is allowed and required.
					},
				},
				RemoveQueueItem{
					Item: queuestore.Item{
						Envelope: NewEnvelope("<id-2>", MessageA1),
					},
				},
				SaveOffset{
					ApplicationKey: "<app-key>",
				},
			}

			var v batchVisitor
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

			var v operationVisitor
			err := batch.AcceptVisitor(context.Background(), v)
			Expect(err).To(MatchError("SaveAggregateMetaData"))
		})
	})
})

type batchVisitor struct {
	operations []Operation
}

func (v *batchVisitor) VisitSaveAggregateMetaData(_ context.Context, op SaveAggregateMetaData) error {
	v.operations = append(v.operations, op)
	return nil
}

func (v *batchVisitor) VisitSaveEvent(_ context.Context, op SaveEvent) error {
	v.operations = append(v.operations, op)
	return nil
}

func (v *batchVisitor) VisitSaveQueueItem(_ context.Context, op SaveQueueItem) error {
	v.operations = append(v.operations, op)
	return nil
}

func (v *batchVisitor) VisitRemoveQueueItem(_ context.Context, op RemoveQueueItem) error {
	v.operations = append(v.operations, op)
	return nil
}

func (v *batchVisitor) VisitSaveOffset(_ context.Context, op SaveOffset) error {
	v.operations = append(v.operations, op)
	return nil
}
