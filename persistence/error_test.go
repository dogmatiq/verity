package persistence_test

import (
	. "github.com/dogmatiq/verity/persistence"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("type UnknownMessageError", func() {
	Describe("func Error()", func() {
		It("includes the message ID of the unknown message", func() {
			err := UnknownMessageError{
				MessageID: "<id>",
			}

			Expect(err.Error()).To(Equal("message with ID '<id>' does not exist"))
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
