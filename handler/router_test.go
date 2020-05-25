package handler_test

import (
	"context"
	"errors"

	. "github.com/dogmatiq/configkit/fixtures"
	. "github.com/dogmatiq/dogma/fixtures"
	. "github.com/dogmatiq/infix/fixtures"
	. "github.com/dogmatiq/infix/handler"
	"github.com/dogmatiq/infix/parcel"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("type MessageTypeRouter", func() {
	var (
		work     *UnitOfWorkStub
		upstream *HandlerStub
		router   MessageTypeRouter
	)

	BeforeEach(func() {
		work = &UnitOfWorkStub{}

		upstream = &HandlerStub{}

		router = MessageTypeRouter{
			MessageAType: upstream,
		}
	})

	It("dispatches to handler based on the message type", func() {
		pcl := NewParcel("<id>", MessageA1)

		upstream.HandleMessageFunc = func(
			_ context.Context,
			w UnitOfWork,
			p parcel.Parcel,
		) error {
			Expect(w).To(BeIdenticalTo(work))
			Expect(p).To(BeIdenticalTo(pcl))
			return errors.New("<error>")
		}

		err := router.HandleMessage(context.Background(), work, pcl)
		Expect(err).To(MatchError("<error>"))
	})

	It("returns an error if there is no upstream handler for the type", func() {
		pcl := NewParcel("<id>", MessageB1)

		upstream.HandleMessageFunc = func(
			_ context.Context,
			w UnitOfWork,
			p parcel.Parcel,
		) error {
			Fail("unexpected call")
			return nil
		}

		err := router.HandleMessage(context.Background(), work, pcl)
		Expect(err).To(MatchError("no route for 'fixtures.MessageB' messages"))
	})
})
