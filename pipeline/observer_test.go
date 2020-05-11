package pipeline_test

import (
	"context"

	. "github.com/dogmatiq/dogma/fixtures"
	. "github.com/dogmatiq/infix/fixtures"
	. "github.com/dogmatiq/infix/pipeline"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("func Observe()", func() {
	var (
		req *PipelineRequestStub
		res *Response
	)

	BeforeEach(func() {
		req, _ = NewPipelineRequestStub(
			NewParcel("<consume>", MessageC1),
			nil,
		)

		res = &Response{}
	})

	It("notifies the observers when the next stage passes", func() {
		p := NewParcel("<produced>", MessageP1)
		res.ExecuteCommand(p)

		called := false
		observe := Observe(
			func(res Result, err error) {
				called = true

				// Verify that it passes the result that's inside the response.
				Expect(res.QueueMessages).To(HaveLen(1))
				Expect(res.QueueMessages[0].Parcel).To(Equal(p))
			},
		)

		err := observe(context.Background(), req, res, pass)
		Expect(err).ShouldNot(HaveOccurred())
		Expect(called).To(BeTrue())
	})

	It("notifies the observers when the next stage fails", func() {
		called := false
		observe := Observe(
			func(res Result, err error) {
				called = true
				Expect(err).To(MatchError("<failed>"))
			},
		)

		err := observe(context.Background(), req, res, fail)
		Expect(err).To(MatchError("<failed>"))
		Expect(called).To(BeTrue())
	})
})
