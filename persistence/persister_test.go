package persistence_test

import (
	. "github.com/dogmatiq/dogma/fixtures"
	. "github.com/dogmatiq/infix/fixtures"
	. "github.com/dogmatiq/infix/persistence"
	"github.com/dogmatiq/infix/persistence/subsystem/aggregatestore"
	"github.com/dogmatiq/infix/persistence/subsystem/queuestore"
	. "github.com/jmalloc/gomegax"
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
				"batch contains multiple operations for the same entity (message <id>)",
			))
		})

		It("does not panic if the batch contains no operations for the same entity", func() {
			batch := Batch{
				SaveAggregateMetaData{
					MetaData: aggregatestore.MetaData{
						HandlerKey: "<handler-key>",
						InstanceID: "<instance>",
					},
				},
				SaveEvent{
					Envelope: NewEnvelope("<id-1>", MessageA1),
				},
				SaveQueueItem{
					Item: queuestore.Item{
						Envelope: NewEnvelope("<id-2>", MessageA1),
					},
				},
				RemoveQueueItem{
					Item: queuestore.Item{
						Envelope: NewEnvelope("<id-3>", MessageA1),
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
})

var _ = Describe("type ConflictError", func() {
	Describe("func Error()", func() {
		It("includes the operation type in the error message", func() {
			err := ConflictError{
				Cause: SaveAggregateMetaData{},
			}

			Expect(err).To(
				MatchError("optimistic concurrency conflict in persistence.SaveAggregateMetaData operation"),
			)
		})
	})
})
