package eventstream_test

import (
	"context"
	"net"
	"time"

	. "github.com/dogmatiq/configkit/fixtures"
	"github.com/dogmatiq/configkit/message"
	. "github.com/dogmatiq/dogma/fixtures"
	. "github.com/dogmatiq/infix/api/messaging/eventstream"
	"github.com/dogmatiq/infix/envelope"
	"github.com/dogmatiq/infix/eventstream"
	. "github.com/dogmatiq/infix/fixtures"
	"github.com/dogmatiq/infix/internal/testing/streamtest"
	"github.com/dogmatiq/infix/persistence/provider/memory"
	. "github.com/dogmatiq/marshalkit/fixtures"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"google.golang.org/grpc"
)

var _ = Describe("type stream (standard test suite)", func() {
	var (
		listener net.Listener
		server   *grpc.Server
	)

	streamtest.Declare(
		func(ctx context.Context, in streamtest.In) streamtest.Out {
			source := &memory.Stream{
				Types: in.MessageTypes,
			}

			var err error
			listener, err = net.Listen("tcp", ":")
			Expect(err).ShouldNot(HaveOccurred())

			server = grpc.NewServer()
			RegisterServer(
				server,
				in.Marshaler,
				map[string]eventstream.Stream{
					"<app-key>": source,
				},
			)

			go server.Serve(listener)

			conn, err := grpc.Dial(
				listener.Addr().String(),
				grpc.WithInsecure(),
			)
			Expect(err).ShouldNot(HaveOccurred())

			return streamtest.Out{
				Stream: NewEventStream(
					"<app-key>",
					conn,
					in.Marshaler,
					0,
				),
				Append: func(_ context.Context, envelopes ...*envelope.Envelope) {
					source.Append(envelopes...)
				},
			}
		},
		func() {
			if listener != nil {
				listener.Close()
			}

			if server != nil {
				server.Stop()
			}
		},
	)
})

var _ = Describe("type stream", func() {
	var (
		ctx      context.Context
		cancel   func()
		listener net.Listener
		server   *grpc.Server
		conn     *grpc.ClientConn
		source   *memory.Stream
		stream   eventstream.Stream
		types    message.TypeSet

		env = NewEnvelope("<message-1>", MessageA1)
	)

	BeforeEach(func() {
		ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)

		types = message.NewTypeSet(MessageAType)
		source = &memory.Stream{
			Types: message.TypesOf(
				env.Message,
			),
		}
		source.Append(env)

		var err error
		listener, err = net.Listen("tcp", ":")
		Expect(err).ShouldNot(HaveOccurred())

		server = grpc.NewServer()
		RegisterServer(
			server,
			Marshaler,
			map[string]eventstream.Stream{
				"<app-key>": source,
			},
		)

		go server.Serve(listener)

		conn, err = grpc.Dial(
			listener.Addr().String(),
			grpc.WithInsecure(),
		)
		Expect(err).ShouldNot(HaveOccurred())

		stream = NewEventStream(
			"<app-key>",
			conn,
			Marshaler,
			0,
		)
	})

	AfterEach(func() {
		if listener != nil {
			listener.Close()
		}

		if server != nil {
			server.Stop()
		}

		if conn != nil {
			conn.Close()
		}

		cancel()
	})

	Describe("func Open()", func() {
		It("returns an error if the making the request fails", func() {
			conn.Close()

			_, err := stream.Open(ctx, 0, types)
			Expect(err).Should(HaveOccurred())
		})
	})

	Describe("func MessageTypes()", func() {
		It("returns an error if the message types can not be queried", func() {
			conn.Close()

			_, err := stream.MessageTypes(ctx)
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
