package eventstream_test

import (
	"context"
	"errors"
	"time"

	"github.com/dogmatiq/configkit"
	. "github.com/dogmatiq/configkit/fixtures"
	"github.com/dogmatiq/configkit/message"
	"github.com/dogmatiq/dodeca/logging"
	. "github.com/dogmatiq/dogma/fixtures"
	. "github.com/dogmatiq/infix/eventstream"
	"github.com/dogmatiq/infix/eventstream/memorystream"
	. "github.com/dogmatiq/infix/fixtures"
	"github.com/dogmatiq/infix/parcel"
	"github.com/dogmatiq/linger/backoff"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"golang.org/x/sync/semaphore"
)

var _ = Describe("type Consumer", func() {
	var (
		ctx       context.Context
		cancel    func()
		mstream   *memorystream.Stream
		stream    *EventStreamStub
		eshandler *EventStreamHandlerStub
		consumer  *Consumer

		event0, event1, event2, event3, event4, event5 *Event
	)

	BeforeEach(func() {
		ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)

		event0 = &Event{
			Offset: 0,
			Parcel: NewParcel("<message-0>", MessageA1),
		}

		event1 = &Event{
			Offset: 1,
			Parcel: NewParcel("<message-1>", MessageB1),
		}

		event2 = &Event{
			Offset: 2,
			Parcel: NewParcel("<message-2>", MessageA2),
		}

		event3 = &Event{
			Offset: 3,
			Parcel: NewParcel("<message-3>", MessageB2),
		}

		event4 = &Event{
			Offset: 4,
			Parcel: NewParcel("<message-4>", MessageA3),
		}

		event5 = &Event{
			Offset: 5,
			Parcel: NewParcel("<message-5>", MessageB3),
		}

		mstream = &memorystream.Stream{
			App: configkit.MustNewIdentity("<app-name>", "<app-key>"),
			Types: message.NewTypeSet(
				MessageAType,
				MessageBType,
			),
		}

		stream = &EventStreamStub{
			Stream: mstream,
		}

		mstream.Add(
			0,
			[]*parcel.Parcel{
				event0.Parcel,
				event1.Parcel,
				event2.Parcel,
				event3.Parcel,
				event4.Parcel,
				event5.Parcel,
			},
		)

		eshandler = &EventStreamHandlerStub{}

		consumer = &Consumer{
			Stream: stream,
			EventTypes: message.NewTypeSet(
				MessageAType,
			),
			Handler:         eshandler,
			BackoffStrategy: backoff.Constant(10 * time.Millisecond),
			Semaphore:       semaphore.NewWeighted(1),
			Logger:          logging.DiscardLogger{},
		}
	})

	AfterEach(func() {
		cancel()
	})

	Describe("func Run()", func() {
		It("passes the filtered events to the handler in order", func() {
			var events []*Event
			eshandler.HandleEventFunc = func(
				_ context.Context,
				_ uint64,
				ev *Event,
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
				[]*Event{
					event0,
					event2,
					event4,
				},
			))
		})

		It("returns if the stream does not produce any relevant events", func() {
			stream.EventTypesFunc = func(
				context.Context,
			) (message.TypeCollection, error) {
				return message.NewTypeSet(MessageCType), nil
			}

			err := consumer.Run(ctx)
			Expect(err).ShouldNot(HaveOccurred())
		})

		It("restarts the consumer if opening the stream returns an error", func() {
			stream.OpenFunc = func(
				context.Context,
				uint64,
				message.TypeCollection,
			) (Cursor, error) {
				stream.OpenFunc = nil

				eshandler.HandleEventFunc = func(
					_ context.Context,
					_ uint64,
					ev *Event,
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

		It("restarts the consumer if querying the stream's message types returns an error", func() {
			stream.EventTypesFunc = func(
				context.Context,
			) (message.TypeCollection, error) {
				stream.EventTypesFunc = nil

				eshandler.HandleEventFunc = func(
					_ context.Context,
					_ uint64,
					ev *Event,
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
				*Event,
			) error {
				eshandler.HandleEventFunc = func(
					_ context.Context,
					_ uint64,
					ev *Event,
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
						configkit.MustNewIdentity("<app-name>", "<app-key>"),
					))
					return 2, nil
				}

				eshandler.HandleEventFunc = func(
					_ context.Context,
					_ uint64,
					ev *Event,
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
					_ *Event,
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
					*Event,
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
						ev *Event,
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
					ev *Event,
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
