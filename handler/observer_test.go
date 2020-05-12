package handler_test

import (
	"errors"

	"github.com/dogmatiq/infix/eventstream"
	. "github.com/dogmatiq/infix/handler"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("func NotifyObservers()", func() {
	It("calls each of the observers in the unit-of-work", func() {
		count := 0

		res := Result{
			Events: []eventstream.Event{
				{
					Offset: 123,
				},
			},
		}

		fn := func(r Result, err error) {
			count++
			Expect(r).To(Equal(res))
			Expect(err).To(MatchError("<error>"))
		}

		work := &UnitOfWork{}
		work.Observe(fn)
		work.Observe(fn)

		NotifyObservers(
			work,
			res,
			errors.New("<error>"),
		)
	})
})
