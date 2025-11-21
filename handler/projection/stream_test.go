package projection_test

import (
	"context"
	"errors"
	"time"

	"github.com/dogmatiq/dodeca/logging"
	"github.com/dogmatiq/dogma"
	. "github.com/dogmatiq/enginekit/enginetest/stubs"
	"github.com/dogmatiq/enginekit/protobuf/identitypb"
	"github.com/dogmatiq/verity/eventstream"
	. "github.com/dogmatiq/verity/fixtures"
	. "github.com/dogmatiq/verity/handler/projection"
	"github.com/dogmatiq/verity/parcel"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ eventstream.Handler = (*StreamAdaptor)(nil)

var _ = Describe("type StreamAdaptor", func() {
	var (
		pcl     parcel.Parcel
		handler *ProjectionMessageHandlerStub
		logger  *logging.BufferedLogger
		adaptor *StreamAdaptor
	)

	BeforeEach(func() {
		pcl = NewParcel("<id>", EventE1)
		handler = &ProjectionMessageHandlerStub{}
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
				identitypb.MustParse("<app-name>", DefaultAppKey),
			)
			Expect(err).ShouldNot(HaveOccurred())
			Expect(offset).To(BeNumerically("==", 0))
		})

		It("returns the checkpoint offset from the projection resource", func() {
			handler.CheckpointOffsetFunc = func(
				_ context.Context,
				id string,
			) (uint64, error) {
				Expect(id).To(Equal(DefaultAppKey))
				return 3, nil
			}

			offset, err := adaptor.NextOffset(
				context.Background(),
				identitypb.MustParse("<app-name>", DefaultAppKey),
			)
			Expect(err).ShouldNot(HaveOccurred())
			Expect(offset).To(BeNumerically("==", 3))
		})

		It("returns an error if the checkpoint offset can not be read", func() {
			handler.CheckpointOffsetFunc = func(
				context.Context,
				string,
			) (uint64, error) {
				return 0, errors.New("<error>")
			}

			_, err := adaptor.NextOffset(
				context.Background(),
				identitypb.MustParse("<app-name>", DefaultAppKey),
			)
			Expect(err).To(MatchError("<error>"))
		})
	})

	Describe("func HandleEvent()", func() {
		It("passes the event to the handler", func() {
			handler.HandleEventFunc = func(
				_ context.Context,
				s dogma.ProjectionEventScope,
				m dogma.Event,
			) (uint64, error) {
				Expect(m).To(Equal(pcl.Message))
				return s.Offset() + 1, nil
			}

			err := adaptor.HandleEvent(
				context.Background(),
				0,
				eventstream.Event{
					Offset: 0,
					Parcel: pcl,
				},
			)
			Expect(err).ShouldNot(HaveOccurred())
		})

		It("passes the correct stream ID and offsets to the handler for the first event", func() {
			handler.HandleEventFunc = func(
				_ context.Context,
				s dogma.ProjectionEventScope,
				_ dogma.Event,
			) (uint64, error) {
				Expect(s.StreamID()).To(Equal(DefaultAppKey))
				Expect(s.CheckpointOffset()).To(BeZero())
				Expect(s.Offset()).To(BeZero())
				return 1, nil
			}

			err := adaptor.HandleEvent(
				context.Background(),
				0,
				eventstream.Event{
					Offset: 0,
					Parcel: pcl,
				},
			)
			Expect(err).ShouldNot(HaveOccurred())
		})

		It("passes the correct stream ID and offsets to the handler for subsequent events", func() {
			handler.HandleEventFunc = func(
				_ context.Context,
				s dogma.ProjectionEventScope,
				_ dogma.Event,
			) (uint64, error) {
				Expect(s.StreamID()).To(Equal(DefaultAppKey))
				Expect(s.CheckpointOffset()).To(BeEquivalentTo(3))
				Expect(s.Offset()).To(BeEquivalentTo(4))
				return 5, nil
			}

			err := adaptor.HandleEvent(
				context.Background(),
				3,
				eventstream.Event{
					Offset: 4,
					Parcel: pcl,
				},
			)
			Expect(err).ShouldNot(HaveOccurred())
		})

		It("uses the adaptor's timeout", func() {
			adaptor.Timeout = 500 * time.Millisecond

			handler.HandleEventFunc = func(
				ctx context.Context,
				s dogma.ProjectionEventScope,
				_ dogma.Event,
			) (uint64, error) {
				dl, ok := ctx.Deadline()
				Expect(ok).To(BeTrue())
				Expect(dl).To(BeTemporally("~", time.Now().Add(500*time.Millisecond)))
				return s.Offset() + 1, nil
			}

			err := adaptor.HandleEvent(
				context.Background(),
				0,
				eventstream.Event{
					Offset: 0,
					Parcel: pcl,
				},
			)
			Expect(err).ShouldNot(HaveOccurred())
		})

		It("falls back to the global default timeout", func() {
			handler.HandleEventFunc = func(
				ctx context.Context,
				s dogma.ProjectionEventScope,
				_ dogma.Event,
			) (uint64, error) {
				dl, ok := ctx.Deadline()
				Expect(ok).To(BeTrue())
				Expect(dl).To(BeTemporally("~", time.Now().Add(DefaultTimeout)))
				return s.Offset() + 1, nil
			}

			err := adaptor.HandleEvent(
				context.Background(),
				0,
				eventstream.Event{
					Offset: 0,
					Parcel: pcl,
				},
			)
			Expect(err).ShouldNot(HaveOccurred())
		})

		It("returns the error when an OCC conflict occurs", func() {
			handler.HandleEventFunc = func(
				_ context.Context,
				s dogma.ProjectionEventScope,
				_ dogma.Event,
			) (uint64, error) {
				return s.Offset() + 123, nil
			}

			err := adaptor.HandleEvent(
				context.Background(),
				0,
				eventstream.Event{
					Offset: 0,
					Parcel: pcl,
				},
			)
			Expect(err).To(MatchError("optimistic concurrency conflict"))
		})

		It("returns an error if the handler returns an error", func() {
			handler.HandleEventFunc = func(
				_ context.Context,
				_ dogma.ProjectionEventScope,
				_ dogma.Event,
			) (uint64, error) {
				return 0, errors.New("<error>")
			}

			err := adaptor.HandleEvent(
				context.Background(),
				0,
				eventstream.Event{
					Offset: 0,
					Parcel: pcl,
				},
			)
			Expect(err).To(MatchError("<error>"))
		})
	})
})
