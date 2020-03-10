package remote_test

import (
	"context"
	"net"
	"time"

	. "github.com/dogmatiq/configkit/fixtures"
	"github.com/dogmatiq/configkit/message"
	. "github.com/dogmatiq/dogma/fixtures"
	"github.com/dogmatiq/infix/envelope"
	. "github.com/dogmatiq/infix/fixtures"
	"github.com/dogmatiq/infix/persistence"
	"github.com/dogmatiq/infix/persistence/internal/streamtest"
	"github.com/dogmatiq/infix/persistence/memory"
	. "github.com/dogmatiq/infix/persistence/remote"
	. "github.com/dogmatiq/marshalkit/fixtures"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"google.golang.org/grpc"
)

var _ = Describe("type stream (standard test suite)", func() {
	var (
		listener net.Listener
		server   *grpc.Server
		source   *memory.Stream
		stream   persistence.Stream
	)

	streamtest.Declare(
		func(ctx context.Context) persistence.Stream {
			source = &memory.Stream{}

			var err error
			listener, err = net.Listen("tcp", ":")
			Expect(err).ShouldNot(HaveOccurred())

			server = grpc.NewServer()
			RegisterEventStreamServer(
				server,
				Marshaler,
				map[string]persistence.Stream{
					"<app-key>": source,
				},
			)

			go server.Serve(listener)

			conn, err := grpc.Dial(
				listener.Addr().String(),
				grpc.WithInsecure(),
			)
			Expect(err).ShouldNot(HaveOccurred())

			stream = NewEventStream("<app-key>", conn, Marshaler, 0)

			return stream
		},
		func() {
			if listener != nil {
				listener.Close()
			}

			if server != nil {
				server.Stop()
			}
		},
		func(ctx context.Context, envelopes ...*envelope.Envelope) {
			source.Append(envelopes...)
		},
	)
})

var _ = Describe("type stream", func() {
	var (
		ctx      context.Context
		cancel   func()
		listener net.Listener
		server   *grpc.Server
		source   *memory.Stream
		stream   persistence.Stream
		types    message.TypeSet

		env = NewEnvelope("<message-1>", MessageA1)
	)

	BeforeEach(func() {
		ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)

		types = message.NewTypeSet(MessageAType)
		source = &memory.Stream{}
		source.Append(env)

		var err error
		listener, err = net.Listen("tcp", ":")
		Expect(err).ShouldNot(HaveOccurred())

		server = grpc.NewServer()
		RegisterEventStreamServer(
			server,
			Marshaler,
			map[string]persistence.Stream{
				"<app-key>": source,
			},
		)

		go server.Serve(listener)

		conn, err := grpc.Dial(
			listener.Addr().String(),
			grpc.WithInsecure(),
		)
		Expect(err).ShouldNot(HaveOccurred())

		stream = NewEventStream("<app-key>", conn, Marshaler, 0)
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

	Describe("func Open()", func() {
		It("returns an error if the making the request fails", func() {
			server.Stop()

			_, err := stream.Open(ctx, 0, types)
			Expect(err).Should(HaveOccurred())
		})
	})

	Describe("type cursor", func() {
		Describe("func Next()", func() {
			It("returns an error if the server returns an invalid envelope", func() {
				env.MetaData.MessageID = ""

				cur, err := stream.Open(ctx, 0, types)
				Expect(err).ShouldNot(HaveOccurred())
				defer cur.Close()

				_, err = cur.Next(ctx)
				Expect(err).Should(HaveOccurred())
			})

			It("returns an error if the cursor is closed while blocked with a message enqueued on the channel", func() {
				cur, err := stream.Open(ctx, 3, types)
				Expect(err).ShouldNot(HaveOccurred())

				// give some time for the message to arrive from the server
				time.Sleep(100 * time.Millisecond)
				cur.Close()

				// wait even longer before unblocking the internal channel
				time.Sleep(100 * time.Millisecond)

				_, err = cur.Next(ctx)
				Expect(err).Should(HaveOccurred())
			})

			It("returns an error if the server goes away while blocked", func() {
				cur, err := stream.Open(ctx, 4, types)
				Expect(err).ShouldNot(HaveOccurred())
				defer cur.Close()

				go func() {
					time.Sleep(100 * time.Millisecond)
					server.Stop()
				}()

				_, err = cur.Next(ctx)
				Expect(err).Should(HaveOccurred())
			})
		})
	})
})
