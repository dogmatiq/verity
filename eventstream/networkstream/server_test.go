package networkstream_test

import (
	"context"
	"net"
	"time"

	"github.com/dogmatiq/configkit/message"
	. "github.com/dogmatiq/dogma/fixtures"
	"github.com/dogmatiq/interopspec/eventstreamspec"
	. "github.com/dogmatiq/marshalkit/fixtures"
	"github.com/dogmatiq/verity/eventstream/memorystream"
	. "github.com/dogmatiq/verity/eventstream/networkstream"
	. "github.com/dogmatiq/verity/fixtures"
	"github.com/dogmatiq/verity/parcel"
	. "github.com/jmalloc/gomegax"
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
		stream   *memorystream.Stream
		listener net.Listener
		server   *grpc.Server
		client   eventstreamspec.StreamAPIClient

		parcel0, parcel1, parcel2, parcel3 parcel.Parcel
	)

	BeforeEach(func() {
		ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)

		parcel0 = NewParcel("<message-0>", MessageA1)
		parcel1 = NewParcel("<message-1>", MessageB1)
		parcel2 = NewParcel("<message-2>", MessageA2)
		parcel3 = NewParcel("<message-3>", MessageB2)

		types := message.TypesOf(
			parcel0.Message,
			parcel1.Message,
			parcel2.Message,
			parcel3.Message,
		)

		stream = &memorystream.Stream{
			Types: types,
		}

		var err error
		listener, err = net.Listen("tcp", ":")
		Expect(err).ShouldNot(HaveOccurred())

		server = grpc.NewServer()
		RegisterServer(
			server,
			Marshaler,
			WithApplication(
				DefaultAppKey,
				stream,
				types,
			),
		)

		go server.Serve(listener)

		conn, err := grpc.Dial(
			listener.Addr().String(),
			grpc.WithInsecure(),
		)
		Expect(err).ShouldNot(HaveOccurred())

		client = eventstreamspec.NewStreamAPIClient(conn)
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
			stream.Append(
				parcel0,
				parcel1,
				parcel2,
				parcel3,
			)
		})

		It("exposes messages from the data-store", func() {
			req := &eventstreamspec.ConsumeRequest{
				ApplicationKey: DefaultAppKey,
				EventTypes: []*eventstreamspec.EventType{
					{
						PortableName: "MessageA",
						MediaTypes:   []string{"application/json; type=MessageA"},
					},
					{
						PortableName: "MessageB",
						MediaTypes:   []string{"application/json; type=MessageB"},
					},
				},
			}

			stream, err := client.Consume(ctx, req)
			Expect(err).ShouldNot(HaveOccurred())

			res, err := stream.Recv()
			Expect(err).ShouldNot(HaveOccurred())
			Expect(res).To(EqualX(
				&eventstreamspec.ConsumeResponse{
					Offset:   0,
					Envelope: parcel0.Envelope,
				},
			))

			res, err = stream.Recv()
			Expect(err).ShouldNot(HaveOccurred())
			Expect(res).To(EqualX(
				&eventstreamspec.ConsumeResponse{
					Offset:   1,
					Envelope: parcel1.Envelope,
				},
			))
		})

		It("honours the initial offset", func() {
			req := &eventstreamspec.ConsumeRequest{
				ApplicationKey: DefaultAppKey,
				StartPoint: &eventstreamspec.ConsumeRequest_Offset{
					Offset: 2,
				},
				EventTypes: []*eventstreamspec.EventType{
					{
						PortableName: "MessageA",
						MediaTypes:   []string{"application/json; type=MessageA"},
					},
					{
						PortableName: "MessageB",
						MediaTypes:   []string{"application/json; type=MessageB"},
					},
				},
			}

			stream, err := client.Consume(ctx, req)
			Expect(err).ShouldNot(HaveOccurred())

			res, err := stream.Recv()
			Expect(err).ShouldNot(HaveOccurred())
			Expect(res).To(EqualX(
				&eventstreamspec.ConsumeResponse{
					Offset:   2,
					Envelope: parcel2.Envelope,
				},
			))
		})

		It("limits results to the supplied event types", func() {
			req := &eventstreamspec.ConsumeRequest{
				ApplicationKey: DefaultAppKey,
				EventTypes: []*eventstreamspec.EventType{
					{
						PortableName: "MessageA",
						MediaTypes:   []string{"application/json; type=MessageA"},
					},
				},
			}

			stream, err := client.Consume(ctx, req)
			Expect(err).ShouldNot(HaveOccurred())

			res, err := stream.Recv()
			Expect(err).ShouldNot(HaveOccurred())
			Expect(res).To(EqualX(
				&eventstreamspec.ConsumeResponse{
					Offset:   0,
					Envelope: parcel0.Envelope,
				},
			))

			res, err = stream.Recv()
			Expect(err).ShouldNot(HaveOccurred())
			Expect(res).To(EqualX(
				&eventstreamspec.ConsumeResponse{
					Offset:   2,
					Envelope: parcel2.Envelope,
				},
			))
		})

		It("transcodes events into a media-type supported by the client", func() {
			req := &eventstreamspec.ConsumeRequest{
				ApplicationKey: DefaultAppKey,
				EventTypes: []*eventstreamspec.EventType{
					{
						PortableName: "MessageA",
						MediaTypes:   []string{"application/cbor; type=MessageA"},
					},
				},
			}

			stream, err := client.Consume(ctx, req)
			Expect(err).ShouldNot(HaveOccurred())

			res, err := stream.Recv()
			Expect(err).ShouldNot(HaveOccurred())
			Expect(res.Envelope.MediaType).To(Equal("application/cbor; type=MessageA"))
			Expect(res.Envelope.Data).To(Equal([]byte("\xa1eValuebA1")))
		})

		It("does not transcode events if the client supports the native media-type", func() {
			req := &eventstreamspec.ConsumeRequest{
				ApplicationKey: DefaultAppKey,
				EventTypes: []*eventstreamspec.EventType{
					{
						PortableName: "MessageA",
						MediaTypes: []string{
							"application/cbor; type=MessageA", // note that CBOR is preferred by the client
							"application/json; type=MessageA", // but JSON is native, so will still be used
						},
					},
				},
			}

			stream, err := client.Consume(ctx, req)
			Expect(err).ShouldNot(HaveOccurred())

			res, err := stream.Recv()
			Expect(err).ShouldNot(HaveOccurred())
			Expect(res.Envelope.MediaType).To(Equal("application/json; type=MessageA"))
			Expect(string(res.Envelope.Data)).To(Equal(`{"Value":"A1"}`))
		})

		It("returns an error if the client and server have no media-types in common", func() {
			req := &eventstreamspec.ConsumeRequest{
				ApplicationKey: DefaultAppKey,
				EventTypes: []*eventstreamspec.EventType{
					{
						PortableName: "MessageA",
						MediaTypes:   []string{"application/unknown; type=MessageA"},
					},
				},
			}

			stream, err := client.Consume(ctx, req)
			Expect(err).ShouldNot(HaveOccurred())

			_, err = stream.Recv()
			s, ok := status.FromError(err)
			Expect(ok).To(BeTrue())
			Expect(s.Message()).To(Equal("none of the requested media-types for 'MessageA' events are supported"))
			Expect(s.Code()).To(Equal(codes.InvalidArgument))
			Expect(s.Details()).To(ContainElement(
				EqualX(
					&eventstreamspec.NoRecognizedMediaTypes{
						PortableName: "MessageA",
					},
				),
			))
		})

		It("returns an INVALID_ARGUMENT error if the application key is empty", func() {
			req := &eventstreamspec.ConsumeRequest{}

			stream, err := client.Consume(ctx, req)
			Expect(err).ShouldNot(HaveOccurred())

			_, err = stream.Recv()
			s, ok := status.FromError(err)
			Expect(ok).To(BeTrue())
			Expect(s.Message()).To(Equal("application key must not be empty"))
			Expect(s.Code()).To(Equal(codes.InvalidArgument))
		})

		It("returns a NOT_FOUND error if the application is not recognized", func() {
			req := &eventstreamspec.ConsumeRequest{
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
					&eventstreamspec.UnrecognizedApplication{
						ApplicationKey: "<unknown>",
					},
				),
			))
		})

		It("returns an INVALID_ARGUMENT error if no event types are requested", func() {
			req := &eventstreamspec.ConsumeRequest{
				ApplicationKey: DefaultAppKey,
			}

			stream, err := client.Consume(ctx, req)
			Expect(err).ShouldNot(HaveOccurred())

			_, err = stream.Recv()
			s, ok := status.FromError(err)
			Expect(ok).To(BeTrue())
			Expect(s.Message()).To(Equal("at least one event type must be consumed"))
			Expect(s.Code()).To(Equal(codes.InvalidArgument))
		})

		It("returns an INVALID_ARGUMENT error if an unrecognized event type is requested", func() {
			req := &eventstreamspec.ConsumeRequest{
				ApplicationKey: DefaultAppKey,
				EventTypes: []*eventstreamspec.EventType{
					{
						PortableName: "<unknown-1>",
					},
					{
						PortableName: "<unknown-2>",
					},
				},
			}

			stream, err := client.Consume(ctx, req)
			Expect(err).ShouldNot(HaveOccurred())

			_, err = stream.Recv()
			s, ok := status.FromError(err)
			Expect(ok).To(BeTrue())
			Expect(s.Message()).To(Equal("one or more unrecognized event types or media-types"))
			Expect(s.Code()).To(Equal(codes.InvalidArgument))
			Expect(s.Details()).To(ContainElements(
				EqualX(
					&eventstreamspec.UnrecognizedEventType{
						PortableName: "<unknown-1>",
					},
				),
				EqualX(
					&eventstreamspec.UnrecognizedEventType{
						PortableName: "<unknown-2>",
					},
				),
			))
		})
	})

	Describe("func EventTypes()", func() {
		It("returns a list of the supported event types", func() {
			req := &eventstreamspec.EventTypesRequest{
				ApplicationKey: DefaultAppKey,
			}

			res, err := client.EventTypes(ctx, req)
			Expect(err).ShouldNot(HaveOccurred())

			Expect(res.GetEventTypes()).To(ConsistOf(
				&eventstreamspec.EventType{
					PortableName: "MessageA",
					MediaTypes: []string{
						"application/json; type=MessageA",
						"application/cbor; type=MessageA",
					},
				},
				&eventstreamspec.EventType{
					PortableName: "MessageB",
					MediaTypes: []string{
						"application/json; type=MessageB",
						"application/cbor; type=MessageB",
					},
				},
			))
		})

		It("returns an INVALID_ARGUMENT error if the application key is empty", func() {
			req := &eventstreamspec.EventTypesRequest{}

			_, err := client.EventTypes(ctx, req)

			s, ok := status.FromError(err)
			Expect(ok).To(BeTrue())
			Expect(s.Message()).To(Equal("application key must not be empty"))
			Expect(s.Code()).To(Equal(codes.InvalidArgument))
		})

		It("returns a NOT_FOUND error if the application is not recognized", func() {
			req := &eventstreamspec.EventTypesRequest{
				ApplicationKey: "<unknown>",
			}

			_, err := client.EventTypes(ctx, req)

			s, ok := status.FromError(err)
			Expect(ok).To(BeTrue())
			Expect(s.Message()).To(Equal("unrecognized application: <unknown>"))
			Expect(s.Code()).To(Equal(codes.NotFound))
			Expect(s.Details()).To(ContainElement(
				EqualX(
					&eventstreamspec.UnrecognizedApplication{
						ApplicationKey: "<unknown>",
					},
				),
			))
		})
	})
})
