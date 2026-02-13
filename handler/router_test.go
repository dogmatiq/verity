package handler_test

import (
	"context"
	"errors"

	. "github.com/dogmatiq/enginekit/enginetest/stubs"
	"github.com/dogmatiq/enginekit/message"
	. "github.com/dogmatiq/verity/fixtures"
	. "github.com/dogmatiq/verity/handler"
	"github.com/dogmatiq/verity/parcel"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("type Router", func() {
	var (
		work                 *UnitOfWorkStub
		upstream1, upstream2 *HandlerStub
		router               Router
	)

	BeforeEach(func() {
		work = &UnitOfWorkStub{}

		upstream1 = &HandlerStub{}
		upstream2 = &HandlerStub{}

		router = Router{
			message.TypeFor[*CommandStub[TypeA]](): []Handler{
				upstream1,
				upstream2,
			},
		}
	})

	It("dispatches to handlers based on the message type", func() {
		pcl := NewParcel("<id>", CommandA1)

		var called1, called2 bool

		upstream1.HandleMessageFunc = func(
			_ context.Context,
			w UnitOfWork,
			p parcel.Parcel,
		) error {
			Expect(w).To(BeIdenticalTo(work))
			Expect(p).To(BeIdenticalTo(pcl))
			called1 = true
			return nil
		}

		upstream2.HandleMessageFunc = func(
			_ context.Context,
			w UnitOfWork,
			p parcel.Parcel,
		) error {
			Expect(w).To(BeIdenticalTo(work))
			Expect(p).To(BeIdenticalTo(pcl))
			called2 = true
			return nil
		}

		err := router.HandleMessage(context.Background(), work, pcl)
		Expect(err).ShouldNot(HaveOccurred())
		Expect(called1).To(BeTrue())
		Expect(called2).To(BeTrue())
	})

	It("stops dispatching if one of the handlers fails", func() {
		pcl := NewParcel("<id>", CommandA1)

		upstream1.HandleMessageFunc = func(
			_ context.Context,
			w UnitOfWork,
			p parcel.Parcel,
		) error {
			return errors.New("<error>")
		}

		upstream2.HandleMessageFunc = func(
			context.Context,
			UnitOfWork,
			parcel.Parcel,
		) error {
			Fail("unexpected call")
			return nil
		}

		err := router.HandleMessage(context.Background(), work, pcl)
		Expect(err).To(MatchError("<error>"))
	})

	It("returns an error if there is no upstream handler for the message type", func() {
		pcl := NewParcel("<id>", CommandB1)

		upstream1.HandleMessageFunc = func(
			context.Context,
			UnitOfWork,
			parcel.Parcel,
		) error {
			Fail("unexpected call")
			return nil
		}

		err := router.HandleMessage(context.Background(), work, pcl)
		Expect(err).To(MatchError("no route for 'stubs.CommandStub[TypeB]' messages"))
	})
})
