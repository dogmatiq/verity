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
		sess  *SessionStub
		scope *Scope
	)

	BeforeEach(func() {
		scope, sess, _ = NewPipelineScope(
			NewParcel("<consume>", MessageC1),
			nil,
		)
	})

	It("injects the stage from the table if there is a match", func() {
		stage := RouteByType(
			map[message.Type]Stage{
				MessageCType: func(ctx context.Context, sc *Scope, next Sink) error {
					return fmt.Errorf("intercepted: %w", next(ctx, sc))
				},
			},
		)

		err := stage(context.Background(), scope, fail)
		Expect(err).To(MatchError("intercepted: <failed>"))
	})

	It("calls the next stage directly if there is no match", func() {
		stage := RouteByType(
			map[message.Type]Stage{
				MessageXType: Terminate(pass),
			},
		)

		err := stage(context.Background(), scope, fail)
		Expect(err).To(MatchError("<failed>"))
	})

	It("returns an error if the parcel cannot be unpacked", func() {
		sess.ParcelFunc = func() (*parcel.Parcel, error) {
			return nil, errors.New("<error>")
		}

		stage := RouteByType(nil)

		err := stage(context.Background(), scope, fail)
		Expect(err).To(MatchError("<error>"))
	})
})
