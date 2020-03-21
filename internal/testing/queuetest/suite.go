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

	// AssumeBlockingDuration specifies how long the tests should wait before
	// assuming a call to Cursor.Next() is successfully blocking, waiting for a
	// new message, as opposed to in the process of "checking" if any messages
	// are already available.
	AssumeBlockingDuration time.Duration

	// Enqueue is a function that enqueues messages.
	Enqueue func(context.Context, ...*envelope.Envelope)
}

const (
	// DefaultTestTimeout is the default test timeout.
	DefaultTestTimeout = 3 * time.Second

	// DefaultAssumeBlockingDuration is the default "assumed blocking duration".
	DefaultAssumeBlockingDuration = 150 * time.Millisecond
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

		envEmptyScheduledFor    *envelope.Envelope
		envScheduledInThePast   *envelope.Envelope
		envScheduledInTheFuture *envelope.Envelope
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

		envEmptyScheduledFor = infixfixtures.NewEnvelope(
			"<empty-scheduled-for>",
			dogmafixtures.MessageA1,
		)

		envScheduledInThePast = infixfixtures.NewEnvelope(
			"<scheduled-in-the-past>",
			dogmafixtures.MessageB1,
			time.Now(),
			time.Now().Add(-1*time.Hour),
		)

		envScheduledInTheFuture = infixfixtures.NewEnvelope(
			"<scheduled-in-the-future>",
			dogmafixtures.MessageC1,
			time.Now(),
			time.Now().Add(1*time.Hour),
		)

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

	ginkgo.Describe("type Queue", func() {
		ginkgo.Describe("func Begin()", func() {
			ginkgo.When("the queue is empty", func() {
				ginkgo.It("blocks", func() {
					ctx, cancel := context.WithTimeout(ctx, out.AssumeBlockingDuration)
					defer cancel()

					_, _, err := out.Queue.Begin(ctx)
					gomega.Expect(err).To(gomega.Equal(context.DeadlineExceeded))
				})

				ginkgo.When("a message is enqueued", func() {
					ginkgo.It("wakes if the message does not have a scheduled-for time", func() {
						go func() {
							time.Sleep(out.AssumeBlockingDuration)
							out.Enqueue(ctx, envEmptyScheduledFor)
						}()

						tx, env, err := out.Queue.Begin(ctx)
						gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
						defer tx.Close()

						gomega.Expect(env).To(gomega.Equal(envEmptyScheduledFor))
					})

					ginkgo.It("wakes if the message is scheduled in the past", func() {
						go func() {
							time.Sleep(out.AssumeBlockingDuration)
							out.Enqueue(ctx, envScheduledInThePast)
						}()

						tx, env, err := out.Queue.Begin(ctx)
						gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
						defer tx.Close()

						gomega.Expect(env).To(gomega.Equal(envScheduledInThePast))
					})

					ginkgo.It("wakes if the message reaches its scheduled-for time", func() {
						envScheduledInTheFuture.ScheduledFor = time.Now().Add(
							out.AssumeBlockingDuration * 2,
						)

						go func() {
							time.Sleep(out.AssumeBlockingDuration)
							out.Enqueue(ctx, envScheduledInTheFuture)
						}()

						tx, env, err := out.Queue.Begin(ctx)
						gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
						defer tx.Close()

						gomega.Expect(env).To(gomega.Equal(envScheduledInTheFuture))
					})

					ginkgo.It("does not wake if the message is scheduled beyond the context deadline", func() {
						go func() {
							time.Sleep(out.AssumeBlockingDuration)
							out.Enqueue(ctx, envScheduledInTheFuture)
						}()

						ctx, cancel := context.WithTimeout(ctx, out.AssumeBlockingDuration*2)
						defer cancel()

						_, _, err := out.Queue.Begin(ctx)
						gomega.Expect(err).To(gomega.Equal(context.DeadlineExceeded))
					})
				})
			})

			ginkgo.When("the queue is not empty", func() {
				ginkgo.It("returns messages that do not have a scheduled-for time", func() {
					out.Enqueue(ctx, envEmptyScheduledFor)

					tx, env, err := out.Queue.Begin(ctx)
					gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
					defer tx.Close()

					gomega.Expect(env).To(gomega.Equal(envEmptyScheduledFor))
				})

				ginkgo.It("returns messages that are scheduled in the past", func() {
					out.Enqueue(ctx, envScheduledInThePast)

					tx, env, err := out.Queue.Begin(ctx)
					gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
					defer tx.Close()

					gomega.Expect(env).To(gomega.Equal(envScheduledInThePast))
				})

				ginkgo.It("blocks until a message reaches its scheduled-for time", func() {
					envScheduledInTheFuture.ScheduledFor = time.Now().Add(out.AssumeBlockingDuration)
					out.Enqueue(ctx, envScheduledInTheFuture)

					tx, env, err := out.Queue.Begin(ctx)
					gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
					defer tx.Close()

					gomega.Expect(env).To(gomega.Equal(envScheduledInTheFuture))
				})

				ginkgo.It("does not return messages that are scheduled beyond the context deadline", func() {
					out.Enqueue(ctx, envScheduledInTheFuture)

					ctx, cancel := context.WithTimeout(ctx, out.AssumeBlockingDuration)
					defer cancel()

					_, _, err := out.Queue.Begin(ctx)
					gomega.Expect(err).To(gomega.Equal(context.DeadlineExceeded))
				})

				ginkgo.It("returns messages that are ready to be handled first", func() {
					out.Enqueue(ctx, envScheduledInTheFuture, envEmptyScheduledFor)

					tx, env, err := out.Queue.Begin(ctx)
					gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
					defer tx.Close()

					gomega.Expect(env).To(gomega.Equal(envEmptyScheduledFor))
				})
			})
		})
	})
}
