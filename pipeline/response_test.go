package pipeline_test

import (
	"context"
	"errors"
	"time"

	. "github.com/dogmatiq/dogma/fixtures"
	"github.com/dogmatiq/infix/draftspecs/envelopespec"
	. "github.com/dogmatiq/infix/fixtures"
	"github.com/dogmatiq/infix/parcel"
	. "github.com/dogmatiq/infix/pipeline"
	. "github.com/jmalloc/gomegax"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("type Response", func() {
	var (
		now time.Time
		pcl *parcel.Parcel
		tx  *TransactionStub
		res *Response
	)

	BeforeEach(func() {
		now = time.Now()
		pcl = NewParcel("<produce>", MessageP1, now, now)
		tx = &TransactionStub{}
		res = &Response{}
	})

	Describe("func RecordEventX()", func() {
		It("persists the event via the transaction", func() {
			called := false
			tx.SaveEventFunc = func(
				_ context.Context,
				env *envelopespec.Envelope,
			) (uint64, error) {
				called = true
				Expect(env).To(EqualX(pcl.Envelope))
				return 0, nil
			}

			_, err := res.RecordEventX(context.Background(), tx, pcl)
			Expect(err).ShouldNot(HaveOccurred())
			Expect(called).To(BeTrue())
		})

		It("returns the offset", func() {
			tx.SaveEventFunc = func(
				context.Context,
				*envelopespec.Envelope,
			) (uint64, error) {
				return 123, nil
			}

			o, err := res.RecordEventX(context.Background(), tx, pcl)
			Expect(err).ShouldNot(HaveOccurred())
			Expect(o).To(BeEquivalentTo(123))
		})

		It("returns an error if the message can not be persisted", func() {
			tx.SaveEventFunc = func(
				context.Context,
				*envelopespec.Envelope,
			) (uint64, error) {
				return 0, errors.New("<error>")
			}

			_, err := res.RecordEventX(context.Background(), tx, pcl)
			Expect(err).To(MatchError("<error>"))
		})
	})
})
