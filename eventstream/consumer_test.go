package eventstream_test

import (
	"context"
	"errors"
	"time"

	"github.com/dogmatiq/configkit"
	"github.com/dogmatiq/configkit/message"
	"github.com/dogmatiq/dodeca/logging"
	"github.com/dogmatiq/enginekit/collections/sets"
	. "github.com/dogmatiq/enginekit/enginetest/stubs"
	"github.com/dogmatiq/linger/backoff"
	. "github.com/dogmatiq/verity/eventstream"
	"github.com/dogmatiq/verity/eventstream/memorystream"
	. "github.com/dogmatiq/verity/fixtures"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"golang.org/x/sync/semaphore"
)

var _ = Describe("type Consumer", func() {
	var (
		ctx       context.Context
		cancel    context.CancelFunc
		mstream   *memorystream.Stream
		stream    *EventStreamStub
		eshandler *EventStreamHandlerStub
		consumer  *Consumer

		event0, event1, event2, event3, event4, event5 Event
	)

	BeforeEach(func() {
		ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
		DeferCleanup(cancel)

		event0 = Event{
			Offset: 0,
			Parcel: NewParcel("<message-0>", EventA1),
		}

		event1 = Event{
			Offset: 1,
			Parcel: NewParcel("<message-1>", EventB1),
		}

		event2 = Event{
			Offset: 2,
			Parcel: NewParcel("<message-2>", EventA2),
		}

		event3 = Event{
			Offset: 3,
			Parcel: NewParcel("<message-3>", EventB2),
		}

		event4 = Event{
			Offset: 4,
			Parcel: NewParcel("<message-4>", EventA3),
		}

		event5 = Event{
			Offset: 5,
			Parcel: NewParcel("<message-5>", EventB3),
		}

		mstream = &memorystream.Stream{
			App: configkit.MustNewIdentity("<app-name>", DefaultAppKey),
			Types: sets.New(
				message.TypeFor[EventStub[TypeA]](),
				message.TypeFor[EventStub[TypeB]](),
			),
		}

		stream = &EventStreamStub{
			Stream: mstream,
		}

		mstream.Append(
			event0.Parcel,
			event1.Parcel,
			event2.Parcel,
			event3.Parcel,
			event4.Parcel,
			event5.Parcel,
		)

		eshandler = &EventStreamHandlerStub{}

		consumer = &Consumer{
			Stream: stream,
			EventTypes: sets.New(
				message.TypeFor[EventStub[TypeA]](),
			),
			Handler:         eshandler,
			BackoffStrategy: backoff.Constant(10 * time.Millisecond),
			Semaphore:       semaphore.NewWeighted(1),
			Logger:          logging.DiscardLogger{},
		}
	})

	Describe("func Run()", func() {
		It("passes the filtered events to the handler in order", func() {
			var events []Event
			eshandler.HandleEventFunc = func(
				_ context.Context,
				_ uint64,
				ev Event,
			) error {
				events = append(events, ev)

				if len(events) == 3 {
					cancel()
				}

				return nil
			}

			err := consumer.Run(ctx)
			Expect(err).To(Equal(context.Canceled))
			Expect(events).To(Equal(
				[]Event{
					event0,
					event2,
					event4,
				},
			))
		})

		It("returns if the stream does not produce any relevant events", func() {
			stream.EventTypesFunc = func(
				context.Context,
			) (*sets.Set[message.Type], error) {
				return sets.New(
					message.TypeFor[EventStub[TypeC]](),
				), nil
			}

			err := consumer.Run(ctx)
			Expect(err).ShouldNot(HaveOccurred())
		})

		It("restarts the consumer if opening the stream returns an error", func() {
			stream.OpenFunc = func(
				context.Context,
				uint64,
				*sets.Set[message.Type],
			) (Cursor, error) {
				stream.OpenFunc = nil

				eshandler.HandleEventFunc = func(
					_ context.Context,
					_ uint64,
					ev Event,
				) error {
					Expect(ev).To(Equal(event0))
					cancel()
					return nil
				}

				return nil, errors.New("<error>")
			}

			err := consumer.Run(ctx)
			Expect(err).To(Equal(context.Canceled))
		})

		It("restarts the consumer if querying the stream's event types returns an error", func() {
			stream.EventTypesFunc = func(
				context.Context,
			) (*sets.Set[message.Type], error) {
				stream.EventTypesFunc = nil

				eshandler.HandleEventFunc = func(
					_ context.Context,
					_ uint64,
					ev Event,
				) error {
					Expect(ev).To(Equal(event0))
					cancel()
					return nil
				}

				return nil, errors.New("<error>")
			}

			err := consumer.Run(ctx)
			Expect(err).To(Equal(context.Canceled))
		})

		It("restarts the consumer when the handler returns an error", func() {
			eshandler.HandleEventFunc = func(
				context.Context,
				uint64,
				Event,
			) error {
				eshandler.HandleEventFunc = func(
					_ context.Context,
					_ uint64,
					ev Event,
				) error {
					Expect(ev).To(Equal(event0))
					cancel()
					return nil
				}

				return errors.New("<error>")
			}

			err := consumer.Run(ctx)
			Expect(err).To(Equal(context.Canceled))
		})

		It("restarts the consumer if the event offset is earlier than the consumed offset", func() {
			// Ensure the consumer tries to consume all event types.
			consumer.EventTypes = sets.New(
				message.TypeFor[EventStub[TypeA]](),
				message.TypeFor[EventStub[TypeB]](),
			)

			// Configure the stream to return an offset before the one we
			// actually wanted.
			stream.OpenFunc = func(
				ctx context.Context,
				offset uint64,
				types *sets.Set[message.Type],
			) (Cursor, error) {
				// Reset the stream to behave normally again on the next
				// attempt at opening a cursor.
				stream.OpenFunc = nil

				return mstream.Open(ctx, offset-1, types)
			}

			// Configure the handler to start at offset 1, just to avoid a
			// negative offset due to the misbehaving stream that we have
			// configured above.
			eshandler.NextOffsetFunc = func(
				context.Context,
				configkit.Identity,
			) (uint64, error) {
				return 1, nil
			}

			eshandler.HandleEventFunc = func(
				_ context.Context,
				_ uint64,
				ev Event,
			) error {
				// We should only ever be passed the event at offset 1.
				Expect(ev).To(Equal(event1))
				cancel()
				return nil
			}

			err := consumer.Run(ctx)
			Expect(err).To(Equal(context.Canceled))
		})

		It("restarts the consumer if the message envelope is invalid", func() {
			// Make event0 envelope invalid.
			event0.Parcel.Envelope.MessageId = ""

			// Configure the handler to start at offset 0, but then try offset 2
			// on the next attempt.
			isRetry := false
			eshandler.NextOffsetFunc = func(
				context.Context,
				configkit.Identity,
			) (uint64, error) {
				if isRetry {
					return 2, nil
				}

				isRetry = true
				return 0, nil
			}

			eshandler.HandleEventFunc = func(
				_ context.Context,
				_ uint64,
				ev Event,
			) error {
				// We should only ever be passed the event at offset 2.
				Expect(ev).To(Equal(event2))
				cancel()
				return nil
			}

			err := consumer.Run(ctx)
			Expect(err).To(Equal(context.Canceled))
		})

		It("restarts the consumer if the event's source application does not match the consumer's application", func() {
			// Make event0 use a different source application to the one that
			// the consumer is expecting to see.
			event0.Parcel.Envelope.SourceApplication.Key = "<different>"

			// Configure the handler to start at offset 0, but then try offset 2
			// on the next attempt.
			isRetry := false
			eshandler.NextOffsetFunc = func(
				context.Context,
				configkit.Identity,
			) (uint64, error) {
				if isRetry {
					return 2, nil
				}

				isRetry = true
				return 0, nil
			}

			eshandler.HandleEventFunc = func(
				_ context.Context,
				_ uint64,
				ev Event,
			) error {
				// We should only ever be passed the event at offset 2.
				Expect(ev).To(Equal(event2))
				cancel()
				return nil
			}

			err := consumer.Run(ctx)
			Expect(err).To(Equal(context.Canceled))
		})

		It("returns if the context is canceled", func() {
			cancel()
			err := consumer.Run(ctx)
			Expect(err).To(Equal(context.Canceled))
		})

		It("returns if the context is canceled while backing off", func() {
			eshandler.NextOffsetFunc = func(
				context.Context,
				configkit.Identity,
			) (uint64, error) {
				return 0, errors.New("<error>")
			}

			consumer.BackoffStrategy = backoff.Constant(10 * time.Second)

			go func() {
				time.Sleep(100 * time.Millisecond)
				cancel()
			}()

			err := consumer.Run(ctx)
			Expect(err).To(Equal(context.Canceled))
		})

		It("returns if the context is canceled while waiting for the sempahore", func() {
			err := consumer.Semaphore.Acquire(ctx, 1)
			Expect(err).ShouldNot(HaveOccurred())
			defer consumer.Semaphore.Release(1)

			go func() {
				time.Sleep(100 * time.Millisecond)
				cancel()
			}()

			err = consumer.Run(ctx)
			Expect(err).To(Equal(context.Canceled))
		})

		Context("optimistic concurrency control", func() {
			It("starts consuming from the next offset", func() {
				eshandler.NextOffsetFunc = func(
					_ context.Context,
					id configkit.Identity,
				) (uint64, error) {
					Expect(id).To(Equal(
						configkit.MustNewIdentity("<app-name>", DefaultAppKey),
					))
					return 2, nil
				}

				eshandler.HandleEventFunc = func(
					_ context.Context,
					_ uint64,
					ev Event,
				) error {
					Expect(ev).To(Equal(event2))
					cancel()
					return nil
				}

				err := consumer.Run(ctx)
				Expect(err).To(Equal(context.Canceled))
			})

			It("passes the correct offset to the handler", func() {
				eshandler.NextOffsetFunc = func(
					context.Context,
					configkit.Identity,
				) (uint64, error) {
					return 2, nil
				}

				eshandler.HandleEventFunc = func(
					_ context.Context,
					o uint64,
					_ Event,
				) error {
					Expect(o).To(BeNumerically("==", 2))
					cancel()
					return nil
				}

				err := consumer.Run(ctx)
				Expect(err).To(Equal(context.Canceled))
			})

			It("restarts the consumer when a conflict occurs", func() {
				eshandler.HandleEventFunc = func(
					context.Context,
					uint64,
					Event,
				) error {
					eshandler.NextOffsetFunc = func(
						context.Context,
						configkit.Identity,
					) (uint64, error) {
						return 2, nil
					}

					eshandler.HandleEventFunc = func(
						_ context.Context,
						_ uint64,
						ev Event,
					) error {
						Expect(ev).To(Equal(event2))
						cancel()
						return nil
					}

					return errors.New("<conflict>")
				}

				err := consumer.Run(ctx)
				Expect(err).To(Equal(context.Canceled))
			})

			It("restarts the consumer when the current offset can not be read", func() {
				eshandler.NextOffsetFunc = func(
					context.Context,
					configkit.Identity,
				) (uint64, error) {
					eshandler.NextOffsetFunc = nil
					return 0, errors.New("<error>")
				}

				eshandler.HandleEventFunc = func(
					_ context.Context,
					_ uint64,
					ev Event,
				) error {
					Expect(ev).To(Equal(event0))
					cancel()

					return nil
				}

				err := consumer.Run(ctx)
				Expect(err).To(Equal(context.Canceled))
			})
		})
	})
})
