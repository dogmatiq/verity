package providertest

import (
	"context"
	"time"

	"github.com/dogmatiq/infix/persistence"
	"github.com/dogmatiq/marshalkit"
	marshalkitfixtures "github.com/dogmatiq/marshalkit/fixtures"
	"github.com/onsi/ginkgo"
)

// In is a container for values that are provided to the provider-specific
// "before" function.
type In struct {
	// Marshaler marshals and unmarshals the test message types, aggregate roots
	// and process roots.
	Marshaler marshalkit.Marshaler
}

// Out is a container for values that are provided by the provider-specific
// "before" function.
type Out struct {
	// Provider is the persistence provider under test.
	Provider persistence.Provider

	// TestTimeout is the maximum duration allowed for each test.
	TestTimeout time.Duration

	// AssumeBlockingDuration specifies how long the tests should wait before
	// assuming a call to Queue.Begin() or Cursor.Next() is successfully
	// blocking, waiting for a new message, as opposed to in the process of
	// "checking" if any messages are already available.
	AssumeBlockingDuration time.Duration
}

const (
	// DefaultTestTimeout is the default test timeout.
	DefaultTestTimeout = 3 * time.Second

	// DefaultAssumeBlockingDuration is the default "assumed blocking duration".
	DefaultAssumeBlockingDuration = 150 * time.Millisecond
)

// Declare declares generic behavioral tests for a specific persistence provider
// implementation.
func Declare(
	before func(context.Context, In) Out,
	after func(),
) {
	var (
		ctx    context.Context
		cancel func()
		in     In
		out    Out
	)

	ginkgo.Context("standard provider test suite", func() {
		ginkgo.BeforeEach(func() {
			setupCtx, cancelSetup := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancelSetup()

			in = In{
				Marshaler: marshalkitfixtures.Marshaler,
			}

			out = before(setupCtx, in)

			if out.TestTimeout <= 0 {
				out.TestTimeout = DefaultTestTimeout
			}

			if out.AssumeBlockingDuration <= 0 {
				out.AssumeBlockingDuration = DefaultAssumeBlockingDuration
			}

			ctx, cancel = context.WithTimeout(context.Background(), out.TestTimeout)
		})

		ginkgo.AfterEach(func() {
			if after != nil {
				after()
			}

			cancel()
		})

		declareProviderTests(&ctx, &in, &out)
		declareDataStoreTests(&ctx, &in, &out)
		// declareEventStreamTests(&ctx, &in, &out)
		declareMessageQueueTests(&ctx, &in, &out)
		declareOffsetRepositoryTests(&ctx, &in, &out)
	})
}
