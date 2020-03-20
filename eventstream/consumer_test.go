package eventstream_test

import (
	"context"
	"errors"
	"time"

	. "github.com/dogmatiq/configkit/fixtures"
	"github.com/dogmatiq/configkit/message"
	"github.com/dogmatiq/dodeca/logging"
	. "github.com/dogmatiq/dogma/fixtures"
	. "github.com/dogmatiq/infix/eventstream"
	"github.com/dogmatiq/infix/fixtures" // can't dot-import due to conflict
	"github.com/dogmatiq/infix/persistence/provider/memory"
	"github.com/dogmatiq/linger/backoff"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("type Consumer", func() {
	var (
		ctx      context.Context
		cancel   func()
		stream   *fixtures.Stream
		handler  *fixtures.StreamHandler
		consumer *Consumer

		env0 = fixtures.NewEnvelope("<message-0>", MessageA1)
		env1 = fixtures.NewEnvelope("<message-1>", MessageB1)
		env2 = fixtures.NewEnvelope("<message-2>", MessageA2)
		env3 = fixtures.NewEnvelope("<message-3>", MessageB2)
		env4 = fixtures.NewEnvelope("<message-4>", MessageA3)
		env5 = fixtures.NewEnvelope("<message-5>", MessageB3)

		event0 = &Event{Offset: 0, Envelope: env0}
		event2 = &Event{Offset: 2, Envelope: env2}
		event4 = &Event{Offset: 4, Envelope: env4}
	)

	BeforeEach(func() {
		ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)

		stream = &fixtures.Stream{
			Memory: memory.Stream{
				AppKey: "<app-key>",
				Types: message.NewTypeSet(
					MessageAType,
					MessageBType,
				),
			},
		}

		stream.Memory.Append(
			env0,
			env1,
			env2,
			env3,
			env4,
			env5,
		)

		handler = &fixtures.StreamHandler{}

		consumer = &Consumer{
			Stream: stream,
			EventTypes: message.NewTypeSet(
				MessageAType,
			),
			Handler:         handler,
			BackoffStrategy: backoff.Constant(10 * time.Millisecond),
			Logger:          logging.DiscardLogger{},
		}
	})

	AfterEach(func() {
		cancel()
	})

	Describe("func Run()", func() {
		It("passes the filtered events to the handler in order", func() {
			var events []*Event
			handler.HandleEventFunc = func(
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
			stream.Memory.Types = message.NewTypeSet(
				MessageCType,
			)

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

				handler.HandleEventFunc = func(
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
			stream.MessageTypesFunc = func(
				context.Context,
			) (message.TypeCollection, error) {
				stream.MessageTypesFunc = nil

				handler.HandleEventFunc = func(
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
			handler.HandleEventFunc = func(
				context.Context,
				uint64,
				*Event,
			) error {
				handler.HandleEventFunc = func(
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
			handler.NextOffsetFunc = func(
				context.Context,
				string,
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

		Context("optimistic concurrency control", func() {
			It("starts consuming from the next offset", func() {
				handler.NextOffsetFunc = func(
					_ context.Context,
					k string,
				) (uint64, error) {
					Expect(k).To(Equal("<app-key>"))
					return 2, nil
				}

				handler.HandleEventFunc = func(
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
				handler.NextOffsetFunc = func(
					_ context.Context,
					k string,
				) (uint64, error) {
					return 2, nil
				}

				handler.HandleEventFunc = func(
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
				handler.HandleEventFunc = func(
					context.Context,
					uint64,
					*Event,
				) error {
					handler.NextOffsetFunc = func(
						_ context.Context,
						k string,
					) (uint64, error) {
						return 2, nil
					}

					handler.HandleEventFunc = func(
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
				handler.NextOffsetFunc = func(
					context.Context,
					string,
				) (uint64, error) {
					handler.NextOffsetFunc = nil
					return 0, errors.New("<error>")
				}

				handler.HandleEventFunc = func(
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