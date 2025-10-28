package streamtest

import (
	"context"
	"sync"
	"time"

	"github.com/dogmatiq/dogma"
	"github.com/dogmatiq/enginekit/collections/sets"
	"github.com/dogmatiq/enginekit/config"
	"github.com/dogmatiq/enginekit/config/runtimeconfig"
	"github.com/dogmatiq/enginekit/enginetest/stubs"
	"github.com/dogmatiq/enginekit/message"
	"github.com/dogmatiq/linger"
	"github.com/dogmatiq/verity/eventstream"
	verityfixtures "github.com/dogmatiq/verity/fixtures"
	"github.com/dogmatiq/verity/parcel"
	"github.com/jmalloc/gomegax"
	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
)

// In is a container for values that are provided to the stream-specific
// "before" function.
type In struct {
	// Application is a test application that is configured to record the events
	// used within the test suite.
	Application *config.Application

	// EventTypes is the set of event types that the test application produces.
	EventTypes *sets.Set[message.Type]
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
	Append func(context.Context, ...parcel.Parcel)
}

const (
	// DefaultTestTimeout is the default test timeout.
	DefaultTestTimeout = 3 * time.Second

	// DefaultAssumeBlockingDuration is the default "assumed blocking duration".
	DefaultAssumeBlockingDuration = 10 * time.Millisecond
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

		event0, event1, event2, event3, event4 eventstream.Event
	)

	ginkgo.BeforeEach(func() {
		event0 = eventstream.Event{
			Offset: 0,
			Parcel: verityfixtures.NewParcel("<message-0>", stubs.EventA1),
		}

		event1 = eventstream.Event{
			Offset: 1,
			Parcel: verityfixtures.NewParcel("<message-1>", stubs.EventB1),
		}

		event2 = eventstream.Event{
			Offset: 2,
			Parcel: verityfixtures.NewParcel("<message-2>", stubs.EventA2),
		}

		event3 = eventstream.Event{
			Offset: 3,
			Parcel: verityfixtures.NewParcel("<message-3>", stubs.EventB2),
		}

		event4 = eventstream.Event{
			Offset: 4,
			Parcel: verityfixtures.NewParcel("<message-4>", stubs.EventC1),
		}
	})

	ginkgo.Context("standard test suite", func() {
		ginkgo.BeforeEach(func() {
			setupCtx, cancelSetup := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancelSetup()

			cfg := runtimeconfig.FromApplication(&stubs.ApplicationStub{
				ConfigureFunc: func(c dogma.ApplicationConfigurer) {
					// use the application identity from the envelope fixtures
					id := event0.Parcel.Envelope.GetSourceApplication()
					c.Identity(id.GetName(), id.GetKey().AsString())

					c.Routes(
						dogma.ViaIntegration(&stubs.IntegrationMessageHandlerStub{
							ConfigureFunc: func(c dogma.IntegrationConfigurer) {
								// use the handler identity from the envelope fixtures
								id := event0.Parcel.Envelope.GetSourceHandler()
								c.Identity(id.GetName(), id.GetKey().AsString())

								c.Routes(
									dogma.HandlesCommand[*stubs.CommandStub[stubs.TypeX]](),
									dogma.RecordsEvent[*stubs.EventStub[stubs.TypeA]](),
									dogma.RecordsEvent[*stubs.EventStub[stubs.TypeB]](),
									dogma.RecordsEvent[*stubs.EventStub[stubs.TypeC]](),
								)
							},
						}),
					)
				},
			})

			in = In{
				cfg,
				cfg.
					RouteSet().
					Filter(
						config.FilterByMessageKind(message.EventKind),
						config.FilterByRouteDirection(config.OutboundDirection),
					).
					MessageTypeSet(),
			}

			out = before(setupCtx, in)

			if out.TestTimeout <= 0 {
				out.TestTimeout = DefaultTestTimeout
			}

			if out.AssumeBlockingDuration <= 0 {
				out.AssumeBlockingDuration = DefaultAssumeBlockingDuration
			}

			ctx, cancel = context.WithTimeout(context.Background(), out.TestTimeout)
			ginkgo.DeferCleanup(cancel)

			if after != nil {
				ginkgo.DeferCleanup(after)
			}
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
						event0.Parcel,
						event1.Parcel,
						event2.Parcel,
						event3.Parcel,
					)
				})

				ginkgo.It("honours the initial offset", func() {
					cur, err := out.Stream.Open(ctx, 2, in.EventTypes)
					gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
					defer cur.Close()

					ev, err := cur.Next(ctx)
					gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
					gomega.Expect(ev).To(gomegax.EqualX(event2))
				})

				ginkgo.It("limits results to the supplied message types", func() {
					types := sets.New(
						message.TypeFor[*stubs.EventStub[stubs.TypeA]](),
					)

					cur, err := out.Stream.Open(ctx, 0, types)
					gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
					defer cur.Close()

					ev, err := cur.Next(ctx)
					gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
					gomega.Expect(ev).To(gomegax.EqualX(event0))

					ev, err = cur.Next(ctx)
					gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
					gomega.Expect(ev).To(gomegax.EqualX(event2))
				})

				ginkgo.It("panics if no event types are specified", func() {
					gomega.Expect(func() {
						types := sets.New[message.Type]()
						cur, err := out.Stream.Open(ctx, 0, types)
						if cur != nil {
							cur.Close()
						}
						gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
					}).To(gomega.Panic())
				})

				ginkgo.It("returns an error if the context is canceled", func() {
					cancel()

					cur, err := out.Stream.Open(ctx, 0, in.EventTypes)
					if cur != nil {
						cur.Close()
					}
					gomega.Expect(err).Should(gomega.HaveOccurred())
				})
			})

			ginkgo.Describe("func EventTypes()", func() {
				ginkgo.It("returns a collection that includes all of the test types", func() {
					// This test ensures that all of the test types are
					// supported by the stream, but does not require that these
					// be the ONLY supported types.
					types, err := out.Stream.EventTypes(ctx)
					gomega.Expect(err).ShouldNot(gomega.HaveOccurred())

					gomega.Expect(
						types.IsSuperset(in.EventTypes),
					).To(
						gomega.BeTrue(),
						"stream does not support all of the required message types",
					)
				})
			})
		})

		ginkgo.Describe("type StreamCursor (interface)", func() {
			ginkgo.Describe("func Next()", func() {
				ginkgo.When("the stream is empty", func() {
					ginkgo.It("blocks", func() {
						cur, err := out.Stream.Open(ctx, 0, in.EventTypes)
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
							event0.Parcel,
							event1.Parcel,
							event2.Parcel,
							event3.Parcel,
						)
					})

					ginkgo.It("returns the messages in order", func() {
						cur, err := out.Stream.Open(ctx, 0, in.EventTypes)
						gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
						defer cur.Close()

						ev, err := cur.Next(ctx)
						gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
						gomega.Expect(ev).To(gomegax.EqualX(event0))

						ev, err = cur.Next(ctx)
						gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
						gomega.Expect(ev).To(gomegax.EqualX(event1))

						ev, err = cur.Next(ctx)
						gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
						gomega.Expect(ev).To(gomegax.EqualX(event2))

						ev, err = cur.Next(ctx)
						gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
						gomega.Expect(ev).To(gomegax.EqualX(event3))
					})

					ginkgo.It("returns an error if the cursor is closed", func() {
						cur, err := out.Stream.Open(ctx, 0, in.EventTypes)
						gomega.Expect(err).ShouldNot(gomega.HaveOccurred())

						cur.Close()

						// Implementations that use signalling channels may take
						// a short while to respond to closed channels, etc.
						time.Sleep(out.AssumeBlockingDuration)

						_, err = cur.Next(ctx)
						gomega.Expect(err).To(gomega.Equal(eventstream.ErrCursorClosed))
					})

					ginkgo.It("returns an error if the context is canceled", func() {
						cur, err := out.Stream.Open(ctx, 4, in.EventTypes)
						gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
						defer cur.Close()

						cancel()

						_, err = cur.Next(ctx)
						gomega.Expect(err).Should(gomega.HaveOccurred())
					})

					ginkgo.When("waiting for a new message", func() {
						ginkgo.When("consuming starts beyond the end of the stream", func() {
							ginkgo.It("wakes if a message is appended", func() {
								// Open a cursor after the offset of the existing messages.
								cur, err := out.Stream.Open(ctx, 4, in.EventTypes)
								gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
								defer cur.Close()

								go func() {
									time.Sleep(out.AssumeBlockingDuration)
									out.Append(ctx, event4.Parcel)
								}()

								ev, err := cur.Next(ctx)
								gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
								gomega.Expect(ev).To(gomegax.EqualX(event4))
							})
						})

						ginkgo.When("consuming starts with messages already on the stream", func() {
							ginkgo.It("does not 'duplicate' the last event", func() {
								// This is a regression test for
								// https://github.com/dogmatiq/verity/issues/194.

								ginkgo.By("opening a cursor at the last event on the stream")

								cur, err := out.Stream.Open(ctx, 3, in.EventTypes)
								gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
								defer cur.Close()

								ginkgo.By("consuming that last event")

								_, err = cur.Next(ctx)
								gomega.Expect(err).ShouldNot(gomega.HaveOccurred())

								go func() {
									ginkgo.By("appending a new event")
									time.Sleep(out.AssumeBlockingDuration)
									out.Append(ctx, event4.Parcel)
								}()

								ginkgo.By("verifying we get the new event")

								ev, err := cur.Next(ctx)
								gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
								gomega.Expect(ev).To(gomegax.EqualX(event4))
							})

							ginkgo.It("does not 'duplicate' the last event when a prior event is filtered", func() {
								// This is a regression test for
								// https://github.com/dogmatiq/verity/issues/194.

								types := sets.New(
									message.TypeFor[*stubs.EventStub[stubs.TypeB]](),
									message.TypeFor[*stubs.EventStub[stubs.TypeC]](),
								)

								ginkgo.By("opening a cursor at an offset with an event that does not match the filter")

								cur, err := out.Stream.Open(ctx, 2, types)
								gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
								defer cur.Close()

								ginkgo.By("consuming the first event after that offset that does match the filte")

								ev, err := cur.Next(ctx)
								gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
								gomega.Expect(ev).To(gomegax.EqualX(event3))

								go func() {
									ginkgo.By("appending a new event")
									time.Sleep(out.AssumeBlockingDuration)
									out.Append(ctx, event4.Parcel)
								}()

								ginkgo.By("verifying we get the new event")

								ev, err = cur.Next(ctx)
								gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
								gomega.Expect(ev).To(gomegax.EqualX(event4))
							})
						})

						ginkgo.It("returns an error if the cursor is closed", func() {
							cur, err := out.Stream.Open(ctx, 4, in.EventTypes)
							gomega.Expect(err).ShouldNot(gomega.HaveOccurred())

							go func() {
								time.Sleep(out.AssumeBlockingDuration)
								cur.Close()
							}()

							_, err = cur.Next(ctx)
							gomega.Expect(err).To(gomega.Equal(eventstream.ErrCursorClosed))
						})

						ginkgo.It("returns an error if the context is canceled", func() {
							cur, err := out.Stream.Open(ctx, 4, in.EventTypes)
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
								go func() {
									defer g.Done()
									defer ginkgo.GinkgoRecover()

									err := linger.SleepX(
										ctx,
										linger.FullJitter,
										out.AssumeBlockingDuration,
									)
									gomega.Expect(err).ShouldNot(gomega.HaveOccurred())

									cur, err := out.Stream.Open(ctx, 4, in.EventTypes)
									gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
									defer cur.Close()

									barrier <- struct{}{}
									ev, err := cur.Next(ctx)
									gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
									gomega.Expect(ev).To(gomegax.EqualX(event4))
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
							out.Append(ctx, event4.Parcel)
						})
					})
				})
			})

			ginkgo.Describe("func Close()", func() {
				ginkgo.It("returns an error if the cursor is already closed", func() {
					cur, err := out.Stream.Open(ctx, 4, in.EventTypes)
					gomega.Expect(err).ShouldNot(gomega.HaveOccurred())

					err = cur.Close()
					gomega.Expect(err).ShouldNot(gomega.HaveOccurred())

					err = cur.Close()
					gomega.Expect(err).To(gomega.Equal(eventstream.ErrCursorClosed))
				})
			})
		})
	})
}
