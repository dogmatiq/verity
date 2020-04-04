package providertest

import (
	"context"
	"time"

	"github.com/dogmatiq/infix/persistence/internal/providertest/common"
	marshalkitfixtures "github.com/dogmatiq/marshalkit/fixtures"
	"github.com/onsi/ginkgo"
)

type (
	// In is a container for values provided by the test suite to the
	// provider-specific initialization code.
	In = common.In

	// Out is a container for values that are provided by the provider-specific
	// initialization code to the test suite.
	Out = common.Out
)

// Declare declares generic behavioral tests for a specific persistence provider
// implementation.
func Declare(
	before func(context.Context, In) Out,
	after func(),
) {
	var (
		tc     common.TestContext
		cancel func()
	)

	ginkgo.Context("persistance provider", func() {
		ginkgo.BeforeEach(func() {
			setupCtx, cancelSetup := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancelSetup()

			tc.In = In{
				Marshaler: marshalkitfixtures.Marshaler,
			}

			tc.Out = before(setupCtx, tc.In)

			if tc.Out.TestTimeout <= 0 {
				tc.Out.TestTimeout = common.DefaultTestTimeout
			}

			tc.Context, cancel = context.WithTimeout(context.Background(), tc.Out.TestTimeout)
		})

		ginkgo.AfterEach(func() {
			if after != nil {
				after()
			}

			cancel()
		})

		declareProviderTests(&tc.Context, &tc.In, &tc.Out)
		declareDataStoreTests(&tc.Context, &tc.In, &tc.Out)
		declareTransactionTests(&tc.Context, &tc.In, &tc.Out)

		declareEventStoreRepositoryTests(&tc.Context, &tc.In, &tc.Out)
		declareEventStoreTransactionTests(&tc.Context, &tc.In, &tc.Out)

		declareQueueStoreRepositoryTests(&tc.Context, &tc.In, &tc.Out)
		declareQueueStoreTransactionTests(&tc.Context, &tc.In, &tc.Out)
	})
}
