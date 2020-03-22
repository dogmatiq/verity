package projection_test

import (
	"context"
	"errors"
	"time"

	"github.com/dogmatiq/configkit"
	"github.com/dogmatiq/dodeca/logging"
	"github.com/dogmatiq/dogma"
	. "github.com/dogmatiq/dogma/fixtures"
	"github.com/dogmatiq/infix/eventstream"
	. "github.com/dogmatiq/infix/fixtures"
	. "github.com/dogmatiq/infix/projection"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ eventstream.Handler = (*StreamAdaptor)(nil)

var _ = Describe("type StreamAdaptor", func() {
	var (
		handler *ProjectionMessageHandler
		logger  *logging.BufferedLogger
		adaptor *StreamAdaptor
		env     = NewEnvelope("<id>", MessageA1)
	)

	BeforeEach(func() {
		handler = &ProjectionMessageHandler{}
		logger = &logging.BufferedLogger{}
		adaptor = &StreamAdaptor{
			Handler: handler,
			Logger:  logger,
		}
	})

	Describe("func NextOffset()", func() {
		It("returns zero when the projection resource does not exist", func() {
			offset, err := adaptor.NextOffset(
				context.Background(),
				configkit.MustNewIdentity("<app-name>", "<app-key>"),
			)
			Expect(err).ShouldNot(HaveOccurred())
			Expect(offset).To(BeNumerically("==", 0))
		})

		It("unmarshals the offset from the projection resource", func() {
			handler.ResourceVersionFunc = func(
				_ context.Context,
				res []byte,
			) ([]byte, error) {
				Expect(res).To(Equal([]byte("<app-key>")))
				return []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02}, nil
			}

			offset, err := adaptor.NextOffset(
				context.Background(),
				configkit.MustNewIdentity("<app-name>", "<app-key>"),
			)
			Expect(err).ShouldNot(HaveOccurred())
			Expect(offset).To(BeNumerically("==", 3))
		})

		It("returns an error if the resource version can not be read", func() {
			handler.ResourceVersionFunc = func(
				_ context.Context,
				res []byte,
			) ([]byte, error) {
				return nil, errors.New("<error>")
			}

			_, err := adaptor.NextOffset(
				context.Background(),
				configkit.MustNewIdentity("<app-name>", "<app-key>"),
			)
			Expect(err).To(MatchError("<error>"))
		})

		It("returns an error when the current version is malformed", func() {
			handler.ResourceVersionFunc = func(
				context.Context,
				[]byte,
			) ([]byte, error) {
				return []byte{00}, nil
			}

			_, err := adaptor.NextOffset(
				context.Background(),
				configkit.MustNewIdentity("<app-name>", "<app-key>"),
			)
			Expect(err).To(MatchError("version is 1 byte(s), expected 0 or 8"))
		})
	})

	Describe("func HandleEvent()", func() {
		It("passes the event to the handler", func() {
			handler.HandleEventFunc = func(
				_ context.Context,
				_, _, _ []byte,
				_ dogma.ProjectionEventScope,
				m dogma.Message,
			) (bool, error) {
				Expect(m).To(Equal(env.Message))
				return true, nil
			}

			err := adaptor.HandleEvent(
				context.Background(),
				0,
				&eventstream.Event{
					Offset:   0,
					Envelope: env,
				},
			)
			Expect(err).ShouldNot(HaveOccurred())
		})

		It("passes the correct resource and versions to the handler for the first event", func() {
			handler.HandleEventFunc = func(
				_ context.Context,
				r, c, n []byte,
				_ dogma.ProjectionEventScope,
				_ dogma.Message,
			) (bool, error) {
				Expect(r).To(Equal([]byte("<app-key>")))
				Expect(c).To(BeEmpty())
				Expect(n).To(Equal([]byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}))
				return true, nil
			}

			err := adaptor.HandleEvent(
				context.Background(),
				0,
				&eventstream.Event{
					Offset:   0,
					Envelope: env,
				},
			)
			Expect(err).ShouldNot(HaveOccurred())
		})

		It("passes the correct resource and versions to the handler for subsequent events", func() {
			handler.HandleEventFunc = func(
				_ context.Context,
				r, c, n []byte,
				_ dogma.ProjectionEventScope,
				_ dogma.Message,
			) (bool, error) {
				Expect(r).To(Equal([]byte("<app-key>")))
				Expect(c).To(Equal([]byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02}))
				Expect(n).To(Equal([]byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x04}))
				return true, nil
			}

			err := adaptor.HandleEvent(
				context.Background(),
				3,
				&eventstream.Event{
					Offset:   4,
					Envelope: env,
				},
			)
			Expect(err).ShouldNot(HaveOccurred())
		})

		It("uses the timeout hint from the handler", func() {
			handler.TimeoutHintFunc = func(m dogma.Message) time.Duration {
				Expect(m).To(Equal(env.Message))
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
				return true, nil
			}

			err := adaptor.HandleEvent(
				context.Background(),
				0,
				&eventstream.Event{
					Offset:   0,
					Envelope: env,
				},
			)
			Expect(err).ShouldNot(HaveOccurred())
		})

		It("falls back to the adaptor's default timeout", func() {
			adaptor.DefaultTimeout = 500 * time.Millisecond

			handler.HandleEventFunc = func(
				ctx context.Context,
				_, _, _ []byte,
				_ dogma.ProjectionEventScope,
				_ dogma.Message,
			) (bool, error) {
				dl, ok := ctx.Deadline()
				Expect(ok).To(BeTrue())
				Expect(dl).To(BeTemporally("~", time.Now().Add(500*time.Millisecond)))
				return true, nil
			}

			err := adaptor.HandleEvent(
				context.Background(),
				0,
				&eventstream.Event{
					Offset:   0,
					Envelope: env,
				},
			)
			Expect(err).ShouldNot(HaveOccurred())
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
				return true, nil
			}

			err := adaptor.HandleEvent(
				context.Background(),
				0,
				&eventstream.Event{
					Offset:   0,
					Envelope: env,
				},
			)
			Expect(err).ShouldNot(HaveOccurred())
		})

		It("returns the error when an OCC conflict occurs", func() {
			handler.HandleEventFunc = func(
				_ context.Context,
				_, _, _ []byte,
				_ dogma.ProjectionEventScope,
				_ dogma.Message,
			) (bool, error) {
				return false, nil
			}

			err := adaptor.HandleEvent(
				context.Background(),
				0,
				&eventstream.Event{
					Offset:   0,
					Envelope: env,
				},
			)
			Expect(err).To(MatchError("optimistic concurrency conflict"))
		})

		It("returns the error if the handler returns an error", func() {
			handler.HandleEventFunc = func(
				_ context.Context,
				_, _, _ []byte,
				_ dogma.ProjectionEventScope,
				_ dogma.Message,
			) (bool, error) {
				return false, errors.New("<error>")
			}

			err := adaptor.HandleEvent(
				context.Background(),
				0,
				&eventstream.Event{
					Offset:   0,
					Envelope: env,
				},
			)
			Expect(err).To(MatchError("<error>"))
		})

		Context("scope", func() {
			It("exposes the time that the event was recorded", func() {
				handler.HandleEventFunc = func(
					_ context.Context,
					_, _, _ []byte,
					s dogma.ProjectionEventScope,
					_ dogma.Message,
				) (bool, error) {
					Expect(s.RecordedAt()).To(BeTemporally("==", env.CreatedAt))
					return true, nil
				}

				err := adaptor.HandleEvent(
					context.Background(),
					0,
					&eventstream.Event{
						Offset:   0,
						Envelope: env,
					},
				)
				Expect(err).ShouldNot(HaveOccurred())
			})

			It("logs messages to the logger", func() {
				handler.HandleEventFunc = func(
					_ context.Context,
					_, _, _ []byte,
					s dogma.ProjectionEventScope,
					_ dogma.Message,
				) (bool, error) {
					s.Log("format %s", "<value>")
					return true, nil
				}

				err := adaptor.HandleEvent(
					context.Background(),
					0,
					&eventstream.Event{
						Offset:   0,
						Envelope: env,
					},
				)
				Expect(err).ShouldNot(HaveOccurred())

				Expect(logger.Messages()).To(ContainElement(
					logging.BufferedLogMessage{
						Message: "format <value>",
					},
				))
			})
		})
	})
})
