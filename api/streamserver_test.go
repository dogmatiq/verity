package api

import (
	"context"
	"net"
	"time"

	. "github.com/dogmatiq/dogma/fixtures"
	"github.com/dogmatiq/infix/envelope"
	. "github.com/dogmatiq/infix/fixtures"
	"github.com/dogmatiq/infix/internal/draftspecs/messagingspec"
	"github.com/dogmatiq/infix/persistence"
	"github.com/dogmatiq/infix/persistence/memory"
	. "github.com/dogmatiq/marshalkit/fixtures"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var _ = Describe("type streamServer", func() {
	var (
		ctx      context.Context
		cancel   func()
		stream   *memory.Stream
		listener net.Listener
		gserver  *grpc.Server
		client   messagingspec.EventStreamClient
	)

	BeforeEach(func() {
		ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)

		stream = &memory.Stream{}

		var err error
		listener, err = net.Listen("tcp", ":")
		Expect(err).ShouldNot(HaveOccurred())

		gserver = grpc.NewServer()
		RegisterEventStreamServer(
			gserver,
			Marshaler,
			map[string]persistence.Stream{
				"<app-key>": stream,
			},
		)

		go gserver.Serve(listener)

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

		if gserver != nil {
			gserver.Stop()
		}

		cancel()
	})

	Describe("func Consume()", func() {
		env1 := NewEnvelope("<message-1>", MessageA1)
		env2 := NewEnvelope("<message-2>", MessageB1)
		env3 := NewEnvelope("<message-3>", MessageA2)
		env4 := NewEnvelope("<message-4>", MessageB2)

		BeforeEach(func() {
			stream.Append(env1, env2, env3, env4)
		})

		It("exposes the messages from the underlying stream", func() {
			req := &messagingspec.ConsumeRequest{
				ApplicationKey: "<app-key>",
				Types:          []string{"MessageA", "MessageB"},
			}

			stream, err := client.Consume(ctx, req)
			Expect(err).ShouldNot(HaveOccurred())

			m, err := stream.Recv()
			Expect(err).ShouldNot(HaveOccurred())
			Expect(m).To(Equal(
				&messagingspec.ConsumeResponse{
					Offset:   0,
					Envelope: envelope.MustMarshal(env1),
				},
			))

			m, err = stream.Recv()
			Expect(err).ShouldNot(HaveOccurred())
			Expect(m).To(Equal(
				&messagingspec.ConsumeResponse{
					Offset:   1,
					Envelope: envelope.MustMarshal(env2),
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

			m, err := stream.Recv()
			Expect(err).ShouldNot(HaveOccurred())
			Expect(m).To(Equal(
				&messagingspec.ConsumeResponse{
					Offset:   2,
					Envelope: envelope.MustMarshal(env3),
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

			m, err := stream.Recv()
			Expect(err).ShouldNot(HaveOccurred())
			Expect(m).To(Equal(
				&messagingspec.ConsumeResponse{
					Offset:   0,
					Envelope: envelope.MustMarshal(env1),
				},
			))

			m, err = stream.Recv()
			Expect(err).ShouldNot(HaveOccurred())
			Expect(m).To(Equal(
				&messagingspec.ConsumeResponse{
					Offset:   2,
					Envelope: envelope.MustMarshal(env3),
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
				&messagingspec.UnrecognizedApplication{
					ApplicationKey: "<unknown>",
				},
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

		It("returns an INVALID_ARGUMENT error if the message type collection contains unrecognised messages", func() {
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
				&messagingspec.UnrecognizedMessage{
					Name: "<unknown-1>",
				},
				&messagingspec.UnrecognizedMessage{
					Name: "<unknown-2>",
				},
			))
		})
	})
})
