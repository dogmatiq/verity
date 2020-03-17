package queuetest

import (
	"context"
	"time"

	configkitfixtures "github.com/dogmatiq/configkit/fixtures"
	"github.com/dogmatiq/configkit/message"
	dogmafixtures "github.com/dogmatiq/dogma/fixtures"
	"github.com/dogmatiq/infix/envelope"
	infixfixtures "github.com/dogmatiq/infix/fixtures"
	"github.com/dogmatiq/infix/persistence"
	"github.com/dogmatiq/marshalkit"
	marshalkitfixtures "github.com/dogmatiq/marshalkit/fixtures"
	"github.com/onsi/ginkgo"
	"github.com/onsi/gomega"
)

// In is a container for values that are provided to the stream-specific
// "before" function from the test-suite.
type In struct {
	// MessageTypes is the set of messages that the test suite will use for
	// testing.
	MessageTypes message.TypeCollection

	// Marshaler a marshaler that supports the test message types.
	Marshaler marshalkit.Marshaler
}

// Out is a container for values that are provided by the stream-specific
// "before" from to the test-suite.
type Out struct {
	// Queue is the queue to be tested.
	Queue persistence.Queue

	// TestTimeout is the maximum duration allowed for each test.
	TestTimeout time.Duration

	// Enqueue is a function that enqueues messages.
	Enqueue func(context.Context, ...*envelope.Envelope)
}

const (
	// DefaultTestTimeout is the default test timeout.
	DefaultTestTimeout = 3 * time.Second
)

// Declare declares generic behavioral tests for a specific queue
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

	ginkgo.BeforeEach(func() {
		setupCtx, cancelSetup := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancelSetup()

		in = In{
			message.NewTypeSet(
				configkitfixtures.MessageAType,
				configkitfixtures.MessageBType,
				configkitfixtures.MessageCType,
			),
			marshalkitfixtures.Marshaler,
		}

		out = before(setupCtx, in)

		if out.TestTimeout <= 0 {
			out.TestTimeout = DefaultTestTimeout
		}

		ctx, cancel = context.WithTimeout(context.Background(), out.TestTimeout)
	})

	ginkgo.AfterEach(func() {
		if after != nil {
			after()
		}

		cancel()
	})

	ginkgo.Describe("type Queue", func() {
		ginkgo.Describe("func Get()", func() {
			ginkgo.It("returns messages that do not have a scheduled-for time", func() {
				env := infixfixtures.NewEnvelope("<id>", dogmafixtures.MessageA1)
				out.Enqueue(ctx, env)

				tx, err := out.Queue.Get(ctx)
				gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
				defer tx.Close()

				gomega.Expect(tx.Envelope()).To(gomega.Equal(env))
			})
		})
	})

	ginkgo.Describe("type QueueTransaction", func() {

	})
}
