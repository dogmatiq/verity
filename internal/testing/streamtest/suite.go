package streamtest

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/dogmatiq/configkit"
	configkitfixtures "github.com/dogmatiq/configkit/fixtures"
	"github.com/dogmatiq/configkit/message"
	"github.com/dogmatiq/dogma"
	dogmafixtures "github.com/dogmatiq/dogma/fixtures"
	"github.com/dogmatiq/infix/envelope"
	"github.com/dogmatiq/infix/eventstream"
	infixfixtures "github.com/dogmatiq/infix/fixtures"
	"github.com/dogmatiq/linger"
	"github.com/dogmatiq/marshalkit"
	marshalkitfixtures "github.com/dogmatiq/marshalkit/fixtures"
	"github.com/onsi/ginkgo"
	"github.com/onsi/gomega"
)

// In is a container for values that are provided to the stream-specific
// "before" function.
type In struct {
	// Application is a test application that is configured to record the events
	// used within the test suite.
	Application configkit.RichApplication

	// MessageTypes is the set of event types that the test application can
	// produce.
	MessageTypes message.TypeCollection

	// Packer is an envelope packer for the test application.
	Packer *envelope.Packer

	// Marshaler marshals and unmarshals the test message types.
	Marshaler marshalkit.Marshaler
}

// Out is a container for values that are provided by the stream-specific
// "before" function.
type Out struct {
	// Stream is the stream under test.
	Stream eventstream.Stream

	// TestTimeout is the maximum duration allowed for each test.
	TestTimeout time.Duration

	// AssumeBlockingDuration specifies how long the tests should wait before
	// assuming a call to Cursor.Next() is successfully blocking, waiting for a
	// new message, as opposed to in the process of "checking" if any messages
	// are already available.
	AssumeBlockingDuration time.Duration

	// Append is a function that appends messages to the stream.
	Append func(context.Context, ...*envelope.Envelope)
}

const (
	// DefaultTestTimeout is the default test timeout.
	DefaultTestTimeout = 3 * time.Second

	// DefaultAssumeBlockingDuration is the default "assumed blocking duration".
	DefaultAssumeBlockingDuration = 150 * time.Millisecond
)

// Declare declares generic behavioral tests for a specific stream
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

		env0 = infixfixtures.NewEnvelope("<message-0>", dogmafixtures.MessageA1)
		env1 = infixfixtures.NewEnvelope("<message-1>", dogmafixtures.MessageB1)
		env2 = infixfixtures.NewEnvelope("<message-2>", dogmafixtures.MessageA2)
		env3 = infixfixtures.NewEnvelope("<message-3>", dogmafixtures.MessageB2)
		env4 = infixfixtures.NewEnvelope("<message-4>", dogmafixtures.MessageC1)

		event0 = &eventstream.Event{Offset: 0, Envelope: env0}
		event1 = &eventstream.Event{Offset: 1, Envelope: env1}
		event2 = &eventstream.Event{Offset: 2, Envelope: env2}
		event3 = &eventstream.Event{Offset: 3, Envelope: env3}
		event4 = &eventstream.Event{Offset: 4, Envelope: env4}
	)

	ginkgo.BeforeEach(func() {
		setupCtx, cancelSetup := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancelSetup()

		cfg := configkit.FromApplication(&dogmafixtures.Application{
			ConfigureFunc: func(c dogma.ApplicationConfigurer) {
				// use the application identity from the envelope fixtures
				id := env0.Source.Application
				c.Identity(id.Name, id.Key)

				c.RegisterIntegration(&dogmafixtures.IntegrationMessageHandler{
					ConfigureFunc: func(c dogma.IntegrationConfigurer) {
						// use the handler identity from the envelope fixtures
						id := env0.Source.Handler
						c.Identity(id.Name, id.Key)

						c.ConsumesCommandType(dogmafixtures.MessageX{})

						c.ProducesEventType(dogmafixtures.MessageA{})
						c.ProducesEventType(dogmafixtures.MessageB{})
						c.ProducesEventType(dogmafixtures.MessageC{})
					},
				})
			},
		})

		m := marshalkitfixtures.Marshaler
		p := envelope.NewPackerForApplication(cfg, m)

		in = In{
			cfg,
			cfg.MessageTypes().Produced.FilterByRole(message.EventRole),
			p,
			m,
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

	ginkgo.Describe("type Stream (interface)", func() {
		ginkgo.Describe("func Application()", func() {
			ginkgo.It("returns the expected identity", func() {
				gomega.Expect(
					out.Stream.Application(),
				).To(gomega.Equal(in.Application.Identity()))
			})
		})

		ginkgo.Describe("func Open()", func() {
			ginkgo.BeforeEach(func() {
				out.Append(
					ctx,
					env0,
					env1,
					env2,
					env3,
				)
			})

			ginkgo.It("honours the initial offset", func() {
				cur, err := out.Stream.Open(ctx, 2, in.MessageTypes)
				gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
				defer cur.Close()

				ev, err := cur.Next(ctx)
				gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
				gomega.Expect(ev).To(gomega.Equal(event2))
			})

			ginkgo.It("limits results to the supplied message types", func() {
				types := message.NewTypeSet(
					configkitfixtures.MessageAType,
				)

				cur, err := out.Stream.Open(ctx, 0, types)
				gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
				defer cur.Close()

				ev, err := cur.Next(ctx)
				gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
				gomega.Expect(ev).To(gomega.Equal(event0))

				ev, err = cur.Next(ctx)
				gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
				gomega.Expect(ev).To(gomega.Equal(event2))
			})

			ginkgo.It("returns an error if the context is canceled", func() {
				cancel()

				_, err := out.Stream.Open(ctx, 0, in.MessageTypes)
				gomega.Expect(err).Should(gomega.HaveOccurred())
			})
		})

		ginkgo.Describe("func MessageTypes()", func() {
			ginkgo.It("returns a collection that includes all of the test types", func() {
				// This test ensures that all of the test types are supported by
				// the stream, but does not require that these be the ONLY
				// supported types.
				types, err := out.Stream.MessageTypes(ctx)
				gomega.Expect(err).ShouldNot(gomega.HaveOccurred())

				in.MessageTypes.Range(func(t message.Type) bool {
					gomega.Expect(types.Has(t)).To(
						gomega.BeTrue(),
						fmt.Sprintf("stream does not support expected message type: %s", t),
					)
					return true
				})
			})
		})
	})

	ginkgo.Describe("type StreamCursor (interface)", func() {
		ginkgo.Describe("func Next()", func() {
			ginkgo.When("the stream is empty", func() {
				ginkgo.It("blocks", func() {
					cur, err := out.Stream.Open(ctx, 0, in.MessageTypes)
					gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
					defer cur.Close()

					ctx, cancel := context.WithTimeout(ctx, out.AssumeBlockingDuration)
					defer cancel()

					_, err = cur.Next(ctx)
					gomega.Expect(err).To(gomega.Equal(context.DeadlineExceeded))
				})
			})

			ginkgo.When("the stream is not empty", func() {
				ginkgo.BeforeEach(func() {
					out.Append(
						ctx,
						env0,
						env1,
						env2,
						env3,
					)
				})

				ginkgo.It("returns the messages in order", func() {
					cur, err := out.Stream.Open(ctx, 0, in.MessageTypes)
					gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
					defer cur.Close()

					ev, err := cur.Next(ctx)
					gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
					gomega.Expect(ev).To(gomega.Equal(event0))

					ev, err = cur.Next(ctx)
					gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
					gomega.Expect(ev).To(gomega.Equal(event1))

					ev, err = cur.Next(ctx)
					gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
					gomega.Expect(ev).To(gomega.Equal(event2))

					ev, err = cur.Next(ctx)
					gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
					gomega.Expect(ev).To(gomega.Equal(event3))
				})

				ginkgo.It("returns an error if the cursor is closed", func() {
					cur, err := out.Stream.Open(ctx, 0, in.MessageTypes)
					gomega.Expect(err).ShouldNot(gomega.HaveOccurred())

					cur.Close()

					_, err = cur.Next(ctx)
					gomega.Expect(err).To(gomega.Equal(eventstream.ErrCursorClosed))
				})

				ginkgo.It("returns an error if the context is canceled", func() {
					cur, err := out.Stream.Open(ctx, 4, in.MessageTypes)
					gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
					defer cur.Close()

					cancel()

					_, err = cur.Next(ctx)
					gomega.Expect(err).Should(gomega.HaveOccurred())
				})

				ginkgo.When("waiting for a new message", func() {
					ginkgo.It("wakes if a message is appended", func() {
						// Open a cursor after the offset of the existing messages.
						cur, err := out.Stream.Open(ctx, 4, in.MessageTypes)
						gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
						defer cur.Close()

						go func() {
							time.Sleep(out.AssumeBlockingDuration)
							out.Append(ctx, env4)
						}()

						ev, err := cur.Next(ctx)
						gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
						gomega.Expect(ev).To(gomega.Equal(event4))
					})

					ginkgo.It("returns an error if the cursor is closed", func() {
						cur, err := out.Stream.Open(ctx, 4, in.MessageTypes)
						gomega.Expect(err).ShouldNot(gomega.HaveOccurred())

						go func() {
							time.Sleep(out.AssumeBlockingDuration)
							cur.Close()
						}()

						_, err = cur.Next(ctx)
						gomega.Expect(err).To(gomega.Equal(eventstream.ErrCursorClosed))
					})

					ginkgo.It("returns an error if the context is canceled", func() {
						cur, err := out.Stream.Open(ctx, 4, in.MessageTypes)
						gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
						defer cur.Close()

						go func() {
							time.Sleep(out.AssumeBlockingDuration)
							cancel()
						}()

						_, err = cur.Next(ctx)
						gomega.Expect(err).Should(gomega.HaveOccurred())
					})

					ginkgo.It("does not compete with other waiting cursors", func() {
						// This test ensures that when there are multiple
						// cursors awaiting a new message they are all woken
						// when a message is appended.

						const cursors = 3

						// barrier is used to delay the append until all of the
						// cursors have started blocking.
						barrier := make(chan struct{}, cursors)

						var g sync.WaitGroup
						defer g.Wait()

						g.Add(cursors)

						// start the cursors
						for i := 0; i < cursors; i++ {
							go func() error {
								linger.SleepX(
									ctx,
									linger.FullJitter,
									out.AssumeBlockingDuration,
								)

								defer g.Done()
								defer ginkgo.GinkgoRecover()

								cur, err := out.Stream.Open(ctx, 4, in.MessageTypes)
								gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
								defer cur.Close()

								barrier <- struct{}{}
								ev, err := cur.Next(ctx)
								gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
								gomega.Expect(ev).To(gomega.Equal(event4))

								return nil
							}()
						}

						// wait for the cursors to signal they are about to block
						for i := 0; i < cursors; i++ {
							select {
							case <-barrier:
							case <-ctx.Done():
								gomega.Expect(ctx.Err()).ShouldNot(gomega.HaveOccurred())
							}
						}

						time.Sleep(out.AssumeBlockingDuration)

						// wake the consumers
						out.Append(ctx, env4)
					})
				})
			})
		})

		ginkgo.Describe("func Close()", func() {
			ginkgo.It("returns an error if the cursor is already closed", func() {
				cur, err := out.Stream.Open(ctx, 4, in.MessageTypes)
				gomega.Expect(err).ShouldNot(gomega.HaveOccurred())

				err = cur.Close()
				gomega.Expect(err).ShouldNot(gomega.HaveOccurred())

				err = cur.Close()
				gomega.Expect(err).To(gomega.Equal(eventstream.ErrCursorClosed))
			})
		})
	})
}
