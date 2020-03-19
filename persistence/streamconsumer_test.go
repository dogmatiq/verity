package persistence_test

import (
	"context"
	"errors"
	"time"

	. "github.com/dogmatiq/configkit/fixtures"
	"github.com/dogmatiq/configkit/message"
	"github.com/dogmatiq/dodeca/logging"
	. "github.com/dogmatiq/dogma/fixtures"
	. "github.com/dogmatiq/infix/fixtures"
	"github.com/dogmatiq/infix/persistence"
	. "github.com/dogmatiq/infix/persistence"
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
		handler  *mockHandler
		consumer *StreamConsumer

		env0 = NewEnvelope("<message-0>", MessageA1)
		env1 = NewEnvelope("<message-1>", MessageB1)
		env2 = NewEnvelope("<message-2>", MessageA2)
		env3 = NewEnvelope("<message-3>", MessageB2)
		env4 = NewEnvelope("<message-4>", MessageA3)
		env5 = NewEnvelope("<message-5>", MessageB3)

		message0 = &persistence.StreamMessage{Offset: 0, Envelope: env0}
		message2 = &persistence.StreamMessage{Offset: 2, Envelope: env2}
		message4 = &persistence.StreamMessage{Offset: 4, Envelope: env4}
	)

	BeforeEach(func() {
		ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)

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

		handler = &mockHandler{}

		consumer = &StreamConsumer{
			ApplicationKey: "<source>",
			Stream:         stream,
			Types: message.NewTypeSet(
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
			var messages []*StreamMessage
			handler.HandleEventFunc = func(
				_ context.Context,
				_ uint64,
				m *StreamMessage,
			) error {
				messages = append(messages, m)

				if len(messages) == 3 {
					cancel()
				}

				return nil
			}

			err := consumer.Run(ctx)
			Expect(err).To(Equal(context.Canceled))
			Expect(messages).To(Equal(
				[]*StreamMessage{
					message0,
					message2,
					message4,
				},
			))
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
				context.Context,
				uint64,
				*StreamMessage,
			) error {
				handler.HandleEventFunc = func(
					_ context.Context,
					_ uint64,
					m *StreamMessage,
				) error {
					Expect(m).To(Equal(message0))
					cancel()
					return nil
				}

				return errors.New("<error>")
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

		Context("optimistic concurrency control", func() {
			It("starts consuming from the next offset", func() {
				handler.NextOffsetFunc = func(
					_ context.Context,
					k string,
				) (uint64, error) {
					Expect(k).To(Equal("<source>"))
					return 2, nil
				}

				handler.HandleEventFunc = func(
					_ context.Context,
					_ uint64,
					m *StreamMessage,
				) error {
					Expect(m).To(Equal(message2))
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
					_ *StreamMessage,
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
					*StreamMessage,
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
						m *StreamMessage,
					) error {
						Expect(m).To(Equal(message2))
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
					m *StreamMessage,
				) error {
					Expect(m).To(Equal(message0))
					cancel()

					return nil
				}

				err := consumer.Run(ctx)
				Expect(err).To(Equal(context.Canceled))
			})
		})
	})
})

type mockHandler struct {
	NextOffsetFunc  func(context.Context, string) (uint64, error)
	HandleEventFunc func(context.Context, uint64, *StreamMessage) error
}

func (h *mockHandler) NextOffset(ctx context.Context, k string) (uint64, error) {
	if h.NextOffsetFunc != nil {
		return h.NextOffsetFunc(ctx, k)
	}

	return 0, nil
}

func (h *mockHandler) HandleEvent(ctx context.Context, o uint64, m *StreamMessage) error {
	if h.HandleEventFunc != nil {
		return h.HandleEventFunc(ctx, o, m)
	}

	return nil
}
