package persistence_test

import (
	. "github.com/dogmatiq/infix/persistence"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

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
