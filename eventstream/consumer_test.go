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
	"github.com/dogmatiq/infix/eventstream"
	. "github.com/dogmatiq/infix/eventstream"
	. "github.com/dogmatiq/infix/fixtures"
	"github.com/dogmatiq/infix/handler"
	"github.com/dogmatiq/linger/backoff"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("type Consumer", func() {
	var (
		ctx                                                  context.Context
		cancel                                               func()
		mstream                                              *MemoryStream
		stream                                               *EventStreamStub
		eshandler                                            *EventStreamHandlerStub
		consumer                                             *Consumer
		parcel0, parcel1, parcel2, parcel3, parcel4, parcel5 *Parcel
	)

	BeforeEach(func() {
		ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)

		parcel0 = &Parcel{
			Offset:   0,
			Envelope: NewEnvelope("<message-0>", MessageA1),
		}

		parcel1 = &Parcel{
			Offset:   1,
			Envelope: NewEnvelope("<message-1>", MessageB1),
		}

		parcel2 = &Parcel{
			Offset:   2,
			Envelope: NewEnvelope("<message-2>", MessageA2),
		}

		parcel3 = &Parcel{
			Offset:   3,
			Envelope: NewEnvelope("<message-3>", MessageB2),
		}

		parcel4 = &Parcel{
			Offset:   4,
			Envelope: NewEnvelope("<message-4>", MessageA3),
		}

		parcel5 = &Parcel{
			Offset:   5,
			Envelope: NewEnvelope("<message-5>", MessageB3),
		}

		mstream = &MemoryStream{
			App: configkit.MustNewIdentity("<app-name>", "<app-key>"),
			Types: message.NewTypeSet(
				MessageAType,
				MessageBType,
			),
		}

		stream = &EventStreamStub{
			Stream: mstream,
		}

		mstream.Append(
			parcel0.Envelope,
			parcel1.Envelope,
			parcel2.Envelope,
			parcel3.Envelope,
			parcel4.Envelope,
			parcel5.Envelope,
		)

		eshandler = &EventStreamHandlerStub{}

		consumer = &Consumer{
			Stream: stream,
			EventTypes: message.NewTypeSet(
				MessageAType,
			),
			Handler:         eshandler,
			BackoffStrategy: backoff.Constant(10 * time.Millisecond),
			Logger:          logging.DiscardLogger{},
		}
	})

	AfterEach(func() {
		cancel()
	})

	Describe("func Run()", func() {
		It("passes the filtered events to the handler in order", func() {
			var parcels []*Parcel
			eshandler.HandleEventFunc = func(
				_ context.Context,
				_ eventstream.Offset,
				p *Parcel,
			) error {
				parcels = append(parcels, p)

				if len(parcels) == 3 {
					cancel()
				}

				return nil
			}

			err := consumer.Run(ctx)
			Expect(err).To(Equal(context.Canceled))
			Expect(parcels).To(Equal(
				[]*Parcel{
					parcel0,
					parcel2,
					parcel4,
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
				eventstream.Offset,
				message.TypeCollection,
			) (Cursor, error) {
				stream.OpenFunc = nil

				eshandler.HandleEventFunc = func(
					_ context.Context,
					_ eventstream.Offset,
					p *Parcel,
				) error {
					Expect(p).To(Equal(parcel0))
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
					_ eventstream.Offset,
					p *Parcel,
				) error {
					Expect(p).To(Equal(parcel0))
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
				eventstream.Offset,
				*Parcel,
			) error {
				eshandler.HandleEventFunc = func(
					_ context.Context,
					_ eventstream.Offset,
					p *Parcel,
				) error {
					Expect(p).To(Equal(parcel0))
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
			) (eventstream.Offset, error) {
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
			consumer.Semaphore = handler.NewSemaphore(1)

			err := consumer.Semaphore.Acquire(ctx)
			Expect(err).ShouldNot(HaveOccurred())
			defer consumer.Semaphore.Release()

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
				) (eventstream.Offset, error) {
					Expect(id).To(Equal(
						configkit.MustNewIdentity("<app-name>", "<app-key>"),
					))
					return 2, nil
				}

				eshandler.HandleEventFunc = func(
					_ context.Context,
					_ eventstream.Offset,
					p *Parcel,
				) error {
					Expect(p).To(Equal(parcel2))
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
				) (eventstream.Offset, error) {
					return 2, nil
				}

				eshandler.HandleEventFunc = func(
					_ context.Context,
					o eventstream.Offset,
					_ *Parcel,
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
					eventstream.Offset,
					*Parcel,
				) error {
					eshandler.NextOffsetFunc = func(
						context.Context,
						configkit.Identity,
					) (eventstream.Offset, error) {
						return 2, nil
					}

					eshandler.HandleEventFunc = func(
						_ context.Context,
						_ eventstream.Offset,
						p *Parcel,
					) error {
						Expect(p).To(Equal(parcel2))
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
				) (eventstream.Offset, error) {
					eshandler.NextOffsetFunc = nil
					return 0, errors.New("<error>")
				}

				eshandler.HandleEventFunc = func(
					_ context.Context,
					_ eventstream.Offset,
					p *Parcel,
				) error {
					Expect(p).To(Equal(parcel0))
					cancel()

					return nil
				}

				err := consumer.Run(ctx)
				Expect(err).To(Equal(context.Canceled))
			})
		})
	})
})
