package providertest

import (
	"context"
	"time"

	marshalkitfixtures "github.com/dogmatiq/marshalkit/fixtures"
	"github.com/onsi/ginkgo/v2"
)

// Declare declares a functional test-suite for a specific persistence.Provider
// implementation.
func Declare(
	before func(context.Context, In) Out,
	after func(),
) {
	var tc TestContext

	ginkgo.Context("persistence provider", func() {
		ginkgo.BeforeEach(func() {
			setupCtx, cancelSetup := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancelSetup()

			tc.In = In{
				Marshaler: marshalkitfixtures.Marshaler,
			}

			tc.Out = before(setupCtx, tc.In)

			if tc.Out.TestTimeout <= 0 {
				tc.Out.TestTimeout = DefaultTestTimeout
			}

			var cancel context.CancelFunc
			tc.Context, cancel = context.WithTimeout(context.Background(), tc.Out.TestTimeout)
			ginkgo.DeferCleanup(cancel)

			if after != nil {
				ginkgo.DeferCleanup(after)
			}
		})

		declareAggregateOperationTests(&tc)
		declareAggregateRepositoryTests(&tc)

		declareEventOperationTests(&tc)
		declareEventRepositoryTests(&tc)

		declareQueueOperationTests(&tc)
		declareQueueRepositoryTests(&tc)

		declareOffsetOperationTests(&tc)
		declareOffsetRepositoryTests(&tc)

		declareProcessOperationTests(&tc)
		declareProcessRepositoryTests(&tc)

		declareProviderTests(&tc)
		declareDataStoreTests(&tc)
	})
}
