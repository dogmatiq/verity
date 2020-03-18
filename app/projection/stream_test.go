package projection_test

import (
	"context"
	"errors"
	"time"

	"github.com/dogmatiq/configkit"
	. "github.com/dogmatiq/configkit/fixtures"
	"github.com/dogmatiq/configkit/message"
	"github.com/dogmatiq/dodeca/logging"
	"github.com/dogmatiq/dogma"
	. "github.com/dogmatiq/dogma/fixtures"
	. "github.com/dogmatiq/infix/app/projection"
	. "github.com/dogmatiq/infix/fixtures"
	"github.com/dogmatiq/infix/persistence/provider/memory"
	"github.com/dogmatiq/linger/backoff"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("type StreamConsumer", func() {
	var (
		ctx      context.Context
		cancel   func()
		stream   *memory.Stream
		handler  *ProjectionMessageHandler
		logger   *logging.BufferedLogger
		consumer *StreamConsumer

		env0 = NewEnvelope("<message-0>", MessageA1)
		env1 = NewEnvelope("<message-1>", MessageB1)
		env2 = NewEnvelope("<message-2>", MessageA2)
		env3 = NewEnvelope("<message-3>", MessageB2)
		env4 = NewEnvelope("<message-4>", MessageA3)
		env5 = NewEnvelope("<message-5>", MessageB3)
	)

	BeforeEach(func() {
		// note that we use a test timeout that is greater than the package's
		// default timeout, so we can test that it is being applied correctly.
		ctx, cancel = context.WithTimeout(context.Background(), DefaultTimeout*2)

		stream = &memory.Stream{
			Types: message.NewTypeSet(
				MessageAType,
				MessageBType,
			),
		}

		stream.Append(
			env0,
			env1,
			env2,
			env3,
			env4,
			env5,
		)

		handler = &ProjectionMessageHandler{
			ConfigureFunc: func(c dogma.ProjectionConfigurer) {
				c.Identity("<proj>", "<proj-key>")
				c.ConsumesEventType(MessageA{})
			},
		}

		logger = &logging.BufferedLogger{}

		consumer = &StreamConsumer{
			ApplicationKey:   "<source>",
			Stream:           stream,
			ProjectionConfig: configkit.FromProjection(handler),
			BackoffStrategy:  backoff.Constant(10 * time.Millisecond),
			Logger:           logger,
		}
	})

	AfterEach(func() {
		cancel()
	})

	Describe("func Run()", func() {
		It("passes the filtered events to the projection in order", func() {
			var messages []dogma.Message
			handler.HandleEventFunc = func(
				_ context.Context,
				_, _, _ []byte,
				_ dogma.ProjectionEventScope,
				m dogma.Message,
			) (bool, error) {
				messages = append(messages, m)

				if len(messages) == 3 {
					cancel()
				}

				return true, nil
			}

			err := consumer.Run(ctx)
			Expect(err).To(Equal(context.Canceled))
			Expect(messages).To(Equal(
				[]dogma.Message{
					MessageA1,
					MessageA2,
					MessageA3,
				},
			))
		})

		It("uses the timeout hint from the handler", func() {
			handler.TimeoutHintFunc = func(dogma.Message) time.Duration {
				return 100 * time.Millisecond
			}

			handler.HandleEventFunc = func(
				ctx context.Context,
				_, _, _ []byte,
				_ dogma.ProjectionEventScope,
				_ dogma.Message,
			) (bool, error) {
				dl, ok := ctx.Deadline()
				Expect(ok).To(BeTrue())
				Expect(dl).To(BeTemporally("~", time.Now().Add(100*time.Millisecond)))
				cancel()
				return true, nil
			}

			err := consumer.Run(ctx)
			Expect(err).To(Equal(context.Canceled))
		})

		It("falls back to the consumer's default timeout", func() {
			consumer.DefaultTimeout = 500 * time.Millisecond

			handler.HandleEventFunc = func(
				ctx context.Context,
				_, _, _ []byte,
				_ dogma.ProjectionEventScope,
				_ dogma.Message,
			) (bool, error) {
				dl, ok := ctx.Deadline()
				Expect(ok).To(BeTrue())
				Expect(dl).To(BeTemporally("~", time.Now().Add(500*time.Millisecond)))
				cancel()
				return true, nil
			}

			err := consumer.Run(ctx)
			Expect(err).To(Equal(context.Canceled))
		})

		It("falls back to the global default timeout", func() {
			handler.HandleEventFunc = func(
				ctx context.Context,
				_, _, _ []byte,
				_ dogma.ProjectionEventScope,
				_ dogma.Message,
			) (bool, error) {
				dl, ok := ctx.Deadline()
				Expect(ok).To(BeTrue())
				Expect(dl).To(BeTemporally("~", time.Now().Add(DefaultTimeout)))
				cancel()
				return true, nil
			}

			err := consumer.Run(ctx)
			Expect(err).To(Equal(context.Canceled))
		})

		It("returns if the stream does not produce any relevant events", func() {
			stream.Types = message.NewTypeSet(
				MessageCType,
			)

			err := consumer.Run(ctx)
			Expect(err).ShouldNot(HaveOccurred())
		})

		It("restarts the consumer when the handler returns an error", func() {
			handler.HandleEventFunc = func(
				_ context.Context,
				_, _, _ []byte,
				_ dogma.ProjectionEventScope,
				_ dogma.Message,
			) (bool, error) {
				handler.HandleEventFunc = func(
					_ context.Context,
					_, _, _ []byte,
					_ dogma.ProjectionEventScope,
					m dogma.Message,
				) (bool, error) {
					Expect(m).To(Equal(MessageA1))
					cancel()
					return true, nil
				}

				return false, errors.New("<error>")
			}

			err := consumer.Run(ctx)
			Expect(err).To(Equal(context.Canceled))
		})

		It("returns if the context is canceled", func() {
			done := make(chan error)
			go func() {
				done <- consumer.Run(ctx)
			}()

			cancel()
			err := <-done
			Expect(err).To(Equal(context.Canceled))
		})

		Context("scope", func() {
			It("exposes the time that the event was recorded", func() {
				handler.HandleEventFunc = func(
					_ context.Context,
					_, _, _ []byte,
					s dogma.ProjectionEventScope,
					_ dogma.Message,
				) (bool, error) {
					Expect(s.RecordedAt()).To(BeTemporally("==", env0.CreatedAt))
					cancel()
					return true, nil
				}

				err := consumer.Run(ctx)
				Expect(err).To(Equal(context.Canceled))
			})

			It("logs messages to the logger", func() {
				handler.HandleEventFunc = func(
					_ context.Context,
					_, _, _ []byte,
					s dogma.ProjectionEventScope,
					_ dogma.Message,
				) (bool, error) {
					s.Log("format %s", "<value>")
					cancel()
					return true, nil
				}

				err := consumer.Run(ctx)
				Expect(err).To(Equal(context.Canceled))

				Expect(logger.Messages()).To(ContainElement(
					logging.BufferedLogMessage{
						Message: "format <value>",
					},
				))
			})
		})

		Context("optimistic concurrency control", func() {
			It("starts consuming from the next offset", func() {
				handler.ResourceVersionFunc = func(
					_ context.Context,
					res []byte,
				) ([]byte, error) {
					Expect(res).To(Equal([]byte("<source>")))
					return []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02}, nil
				}

				handler.HandleEventFunc = func(
					_ context.Context,
					_, _, _ []byte,
					_ dogma.ProjectionEventScope,
					m dogma.Message,
				) (bool, error) {
					Expect(m).To(Equal(MessageA3))
					cancel()
					return true, nil
				}

				err := consumer.Run(ctx)
				Expect(err).To(Equal(context.Canceled))
			})

			It("passes the correct resource and versions to the handler", func() {
				handler.ResourceVersionFunc = func(
					_ context.Context,
					res []byte,
				) ([]byte, error) {
					return []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02}, nil
				}

				handler.HandleEventFunc = func(
					_ context.Context,
					r, c, n []byte,
					_ dogma.ProjectionEventScope,
					_ dogma.Message,
				) (bool, error) {
					Expect(r).To(Equal([]byte("<source>")))
					Expect(c).To(Equal([]byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02}))
					Expect(n).To(Equal([]byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x04}))
					cancel()
					return true, nil
				}

				err := consumer.Run(ctx)
				Expect(err).To(Equal(context.Canceled))
			})

			It("passes the correct resource and versions to the handler when the resource does not exist", func() {
				handler.ResourceVersionFunc = func(
					_ context.Context,
					res []byte,
				) ([]byte, error) {
					return nil, nil
				}

				handler.HandleEventFunc = func(
					_ context.Context,
					r, c, n []byte,
					_ dogma.ProjectionEventScope,
					_ dogma.Message,
				) (bool, error) {
					Expect(r).To(Equal([]byte("<source>")))
					Expect(c).To(BeEmpty())
					Expect(n).To(Equal([]byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}))
					cancel()
					return true, nil
				}

				err := consumer.Run(ctx)
				Expect(err).To(Equal(context.Canceled))
			})

			It("restarts the consumer when a conflict occurs", func() {
				handler.HandleEventFunc = func(
					_ context.Context,
					_, _, _ []byte,
					_ dogma.ProjectionEventScope,
					_ dogma.Message,
				) (bool, error) {
					handler.ResourceVersionFunc = func(
						_ context.Context,
						res []byte,
					) ([]byte, error) {
						return []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02}, nil
					}

					handler.HandleEventFunc = func(
						_ context.Context,
						_, _, _ []byte,
						_ dogma.ProjectionEventScope,
						m dogma.Message,
					) (bool, error) {
						Expect(m).To(Equal(MessageA3))
						cancel()
						return true, nil
					}

					return false, nil
				}

				err := consumer.Run(ctx)
				Expect(err).To(Equal(context.Canceled))
			})

			It("restarts the consumer when the current version is malformed", func() {
				handler.ResourceVersionFunc = func(
					context.Context,
					[]byte,
				) ([]byte, error) {
					handler.ResourceVersionFunc = nil
					return []byte{00}, nil
				}

				handler.HandleEventFunc = func(
					_ context.Context,
					_, _, _ []byte,
					_ dogma.ProjectionEventScope,
					m dogma.Message,
				) (bool, error) {
					Expect(m).To(Equal(MessageA1))
					cancel()

					return false, nil
				}

				err := consumer.Run(ctx)
				Expect(err).To(Equal(context.Canceled))
			})

			It("restarts the consumer when the current version can not be read", func() {
				handler.ResourceVersionFunc = func(
					context.Context,
					[]byte,
				) ([]byte, error) {
					handler.ResourceVersionFunc = nil
					return nil, errors.New("<error>")
				}

				handler.HandleEventFunc = func(
					_ context.Context,
					_, _, _ []byte,
					_ dogma.ProjectionEventScope,
					m dogma.Message,
				) (bool, error) {
					Expect(m).To(Equal(MessageA1))
					cancel()

					return false, nil
				}

				err := consumer.Run(ctx)
				Expect(err).To(Equal(context.Canceled))
			})
		})
	})
})
