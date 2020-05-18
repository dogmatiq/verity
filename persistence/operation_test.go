package persistence_test

import (
	"context"
	"errors"

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
				var v visitor
				err := op.AcceptVisitor(context.Background(), v)
				Expect(err).To(MatchError(expect))
			},
			Entry("type SaveAggregateMetaData", SaveAggregateMetaData{}, "SaveAggregateMetaData"),
			Entry("type SaveEvent", SaveEvent{}, "SaveEvent"),
			Entry("type SaveQueueItem", SaveQueueItem{}, "SaveQueueItem"),
			Entry("type RemoveQueueItem", RemoveQueueItem{}, "RemoveQueueItem"),
			Entry("type SaveOffset", SaveOffset{}, "SaveOffset"),
		)
	})
})

type visitor struct {
}

func (visitor) VisitSaveAggregateMetaData(_ context.Context, op SaveAggregateMetaData) error {
	return errors.New("SaveAggregateMetaData")
}

func (visitor) VisitSaveEvent(_ context.Context, op SaveEvent) error {
	return errors.New("SaveEvent")
}

func (visitor) VisitSaveQueueItem(_ context.Context, op SaveQueueItem) error {
	return errors.New("SaveQueueItem")
}

func (visitor) VisitRemoveQueueItem(_ context.Context, op RemoveQueueItem) error {
	return errors.New("RemoveQueueItem")
}

func (visitor) VisitSaveOffset(_ context.Context, op SaveOffset) error {
	return errors.New("SaveOffset")
}