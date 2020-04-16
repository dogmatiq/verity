package pipeline_test

import (
	"context"
	"errors"
	"fmt"

	. "github.com/dogmatiq/configkit/fixtures"
	"github.com/dogmatiq/configkit/message"
	. "github.com/dogmatiq/dogma/fixtures"
	. "github.com/dogmatiq/infix/fixtures"
	"github.com/dogmatiq/infix/parcel"
	. "github.com/dogmatiq/infix/pipeline"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("func RouteByType()", func() {
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

	It("injects the stage from the table if there is a match", func() {
		stage := RouteByType(
			map[message.Type]Stage{
				MessageCType: func(ctx context.Context, req Request, res *Response, next Sink) error {
					return fmt.Errorf("intercepted: %w", next(ctx, req, res))
				},
			},
		)

		err := stage(context.Background(), req, res, fail)
		Expect(err).To(MatchError("intercepted: <failed>"))
	})

	It("calls the next stage directly if there is no match", func() {
		stage := RouteByType(
			map[message.Type]Stage{
				MessageXType: Terminate(pass),
			},
		)

		err := stage(context.Background(), req, res, fail)
		Expect(err).To(MatchError("<failed>"))
	})

	It("returns an error if the parcel cannot be unpacked", func() {
		req.ParcelFunc = func() (*parcel.Parcel, error) {
			return nil, errors.New("<error>")
		}

		stage := RouteByType(nil)

		err := stage(context.Background(), req, res, fail)
		Expect(err).To(MatchError("<error>"))
	})
})
