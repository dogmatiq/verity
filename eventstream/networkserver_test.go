package eventstream_test

import (
	"context"
	"errors"
	"net"
	"time"

	"github.com/dogmatiq/configkit/message"
	. "github.com/dogmatiq/dogma/fixtures"
	"github.com/dogmatiq/infix/draftspecs/messagingspec"
	"github.com/dogmatiq/infix/eventstream"
	. "github.com/dogmatiq/infix/eventstream"
	. "github.com/dogmatiq/infix/fixtures"
	. "github.com/dogmatiq/infix/internal/x/gomegax"
	"github.com/dogmatiq/infix/parcel"
	. "github.com/dogmatiq/marshalkit/fixtures"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var _ = Describe("type server", func() {
	var (
		ctx      context.Context
		cancel   func()
		mstream  *MemoryStream
		stream   *EventStreamStub
		listener net.Listener
		server   *grpc.Server
		client   messagingspec.EventStreamClient

		parcel0, parcel1, parcel2, parcel3 *parcel.Parcel
	)

	BeforeEach(func() {
		ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)

		parcel0 = NewParcel("<message-0>", MessageA1)
		parcel1 = NewParcel("<message-1>", MessageB1)
		parcel2 = NewParcel("<message-2>", MessageA2)
		parcel3 = NewParcel("<message-3>", MessageB2)

		mstream = &MemoryStream{
			Types: message.TypesOf(
				parcel0.Message,
				parcel1.Message,
				parcel2.Message,
				parcel3.Message,
			),
		}

		stream = &EventStreamStub{
			Stream: mstream,
		}

		var err error
		listener, err = net.Listen("tcp", ":")
		Expect(err).ShouldNot(HaveOccurred())

		server = grpc.NewServer()
		RegisterServer(
			server,
			Marshaler,
			map[string]eventstream.Stream{
				"<app-key>": stream,
			},
		)

		go server.Serve(listener)

		conn, err := grpc.Dial(
			listener.Addr().String(),
			grpc.WithInsecure(),
		)
		Expect(err).ShouldNot(HaveOccurred())

		client = messagingspec.NewEventStreamClient(conn)
	})

	AfterEach(func() {
		if listener != nil {
			listener.Close()
		}

		if server != nil {
			server.Stop()
		}

		cancel()
	})

	Describe("func Consume()", func() {
		BeforeEach(func() {
			mstream.Append(
				parcel0,
				parcel1,
				parcel2,
				parcel3,
			)
		})

		It("exposes the messages from the underlying stream", func() {
			req := &messagingspec.ConsumeRequest{
				ApplicationKey: "<app-key>",
				Types:          []string{"MessageA", "MessageB"},
			}

			stream, err := client.Consume(ctx, req)
			Expect(err).ShouldNot(HaveOccurred())

			res, err := stream.Recv()
			Expect(err).ShouldNot(HaveOccurred())
			Expect(res).To(EqualX(
				&messagingspec.ConsumeResponse{
					Offset:   0,
					Envelope: parcel0.Envelope,
				},
			))

			res, err = stream.Recv()
			Expect(err).ShouldNot(HaveOccurred())
			Expect(res).To(EqualX(
				&messagingspec.ConsumeResponse{
					Offset:   1,
					Envelope: parcel1.Envelope,
				},
			))
		})

		It("honours the initial offset", func() {
			req := &messagingspec.ConsumeRequest{
				ApplicationKey: "<app-key>",
				Offset:         2,
				Types:          []string{"MessageA", "MessageB"},
			}

			stream, err := client.Consume(ctx, req)
			Expect(err).ShouldNot(HaveOccurred())

			res, err := stream.Recv()
			Expect(err).ShouldNot(HaveOccurred())
			Expect(res).To(EqualX(
				&messagingspec.ConsumeResponse{
					Offset:   2,
					Envelope: parcel2.Envelope,
				},
			))
		})

		It("limits results to the supplied message types", func() {
			req := &messagingspec.ConsumeRequest{
				ApplicationKey: "<app-key>",
				Types:          []string{"MessageA"},
			}

			stream, err := client.Consume(ctx, req)
			Expect(err).ShouldNot(HaveOccurred())

			res, err := stream.Recv()
			Expect(err).ShouldNot(HaveOccurred())
			Expect(res).To(EqualX(
				&messagingspec.ConsumeResponse{
					Offset:   0,
					Envelope: parcel0.Envelope,
				},
			))

			res, err = stream.Recv()
			Expect(err).ShouldNot(HaveOccurred())
			Expect(res).To(EqualX(
				&messagingspec.ConsumeResponse{
					Offset:   2,
					Envelope: parcel2.Envelope,
				},
			))
		})

		It("returns an INVALID_ARGUMENT error if the application key is empty", func() {
			req := &messagingspec.ConsumeRequest{}

			stream, err := client.Consume(ctx, req)
			Expect(err).ShouldNot(HaveOccurred())

			_, err = stream.Recv()
			s, ok := status.FromError(err)
			Expect(ok).To(BeTrue())
			Expect(s.Message()).To(Equal("application key must not be empty"))
			Expect(s.Code()).To(Equal(codes.InvalidArgument))
		})

		It("returns a NOT_FOUND error if the application is not recognized", func() {
			req := &messagingspec.ConsumeRequest{
				ApplicationKey: "<unknown>",
			}

			stream, err := client.Consume(ctx, req)
			Expect(err).ShouldNot(HaveOccurred())

			_, err = stream.Recv()
			s, ok := status.FromError(err)
			Expect(ok).To(BeTrue())
			Expect(s.Message()).To(Equal("unrecognized application: <unknown>"))
			Expect(s.Code()).To(Equal(codes.NotFound))
			Expect(s.Details()).To(ContainElement(
				EqualX(
					&messagingspec.UnrecognizedApplication{
						ApplicationKey: "<unknown>",
					},
				),
			))
		})

		It("returns an INVALID_ARGUMENT error if the message type collection is empty", func() {
			req := &messagingspec.ConsumeRequest{
				ApplicationKey: "<app-key>",
			}

			stream, err := client.Consume(ctx, req)
			Expect(err).ShouldNot(HaveOccurred())

			_, err = stream.Recv()
			s, ok := status.FromError(err)
			Expect(ok).To(BeTrue())
			Expect(s.Message()).To(Equal("message types can not be empty"))
			Expect(s.Code()).To(Equal(codes.InvalidArgument))
		})

		It("returns an INVALID_ARGUMENT error if the message type collection contains unrecognized messages", func() {
			req := &messagingspec.ConsumeRequest{
				ApplicationKey: "<app-key>",
				Types:          []string{"<unknown-1>", "<unknown-2>"},
			}

			stream, err := client.Consume(ctx, req)
			Expect(err).ShouldNot(HaveOccurred())

			_, err = stream.Recv()
			s, ok := status.FromError(err)
			Expect(ok).To(BeTrue())
			Expect(s.Message()).To(Equal("unrecognized message type(s)"))
			Expect(s.Code()).To(Equal(codes.InvalidArgument))
			Expect(s.Details()).To(ContainElements(
				EqualX(
					&messagingspec.UnrecognizedMessage{
						Name: "<unknown-1>",
					},
				),
				EqualX(
					&messagingspec.UnrecognizedMessage{
						Name: "<unknown-2>",
					},
				),
			))
		})
	})

	Describe("func EventTypes()", func() {
		It("returns an INVALID_ARGUMENT error if the application key is empty", func() {
			req := &messagingspec.MessageTypesRequest{}

			_, err := client.EventTypes(ctx, req)

			s, ok := status.FromError(err)
			Expect(ok).To(BeTrue())
			Expect(s.Message()).To(Equal("application key must not be empty"))
			Expect(s.Code()).To(Equal(codes.InvalidArgument))
		})

		It("returns a NOT_FOUND error if the application is not recognized", func() {
			req := &messagingspec.MessageTypesRequest{
				ApplicationKey: "<unknown>",
			}

			_, err := client.EventTypes(ctx, req)

			s, ok := status.FromError(err)
			Expect(ok).To(BeTrue())
			Expect(s.Message()).To(Equal("unrecognized application: <unknown>"))
			Expect(s.Code()).To(Equal(codes.NotFound))
			Expect(s.Details()).To(ContainElement(
				EqualX(
					&messagingspec.UnrecognizedApplication{
						ApplicationKey: "<unknown>",
					},
				),
			))
		})

		It("returns an error if the underlying stream returns an error", func() {
			stream.EventTypesFunc = func(
				context.Context,
			) (message.TypeCollection, error) {
				return nil, errors.New("<error>")
			}

			req := &messagingspec.MessageTypesRequest{
				ApplicationKey: "<app-key>",
			}

			_, err := client.EventTypes(ctx, req)

			s, ok := status.FromError(err)
			Expect(ok).To(BeTrue())
			Expect(s.Message()).To(Equal("<error>"))
			Expect(s.Code()).To(Equal(codes.Unknown))
		})
	})
})
