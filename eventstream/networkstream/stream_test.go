package networkstream_test

import (
	"context"
	"net"
	"time"

	"github.com/dogmatiq/configkit"
	. "github.com/dogmatiq/configkit/fixtures"
	"github.com/dogmatiq/configkit/message"
	. "github.com/dogmatiq/dogma/fixtures"
	"github.com/dogmatiq/infix/eventstream/internal/streamtest"
	"github.com/dogmatiq/infix/eventstream/memorystream"
	. "github.com/dogmatiq/infix/eventstream/networkstream"
	. "github.com/dogmatiq/infix/fixtures"
	"github.com/dogmatiq/infix/parcel"
	. "github.com/dogmatiq/marshalkit/fixtures"
	"github.com/dogmatiq/transportspec"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"google.golang.org/grpc"
)

var _ = Describe("type Stream", func() {
	var (
		stream   *memorystream.Stream
		listener net.Listener
		server   *grpc.Server
	)

	streamtest.Declare(
		func(ctx context.Context, in streamtest.In) streamtest.Out {
			stream = &memorystream.Stream{
				Types: in.EventTypes,
			}

			var err error
			listener, err = net.Listen("tcp", ":")
			Expect(err).ShouldNot(HaveOccurred())

			server = grpc.NewServer()
			RegisterServer(
				server,
				in.Marshaler,
				WithApplication(
					"<app-key>",
					stream,
					in.EventTypes,
				),
			)

			go server.Serve(listener)

			conn, err := grpc.Dial(
				listener.Addr().String(),
				grpc.WithInsecure(),
			)
			Expect(err).ShouldNot(HaveOccurred())

			return streamtest.Out{
				Stream: &Stream{
					App:       in.Application.Identity(),
					Client:    transportspec.NewEventStreamClient(conn),
					Marshaler: in.Marshaler,
				},
				Append: func(ctx context.Context, parcels ...parcel.Parcel) {
					stream.Append(parcels...)
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

var _ = Describe("type Stream", func() {
	var (
		ctx      context.Context
		cancel   func()
		mstream  *memorystream.Stream
		listener net.Listener
		server   *grpc.Server
		conn     *grpc.ClientConn
		stream   *Stream
		types    message.TypeSet
		pcl      parcel.Parcel
	)

	BeforeEach(func() {
		ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)

		mstream = &memorystream.Stream{}

		pcl = NewParcel("<message-1>", MessageA1)

		types = message.NewTypeSet(MessageAType)

		var err error
		listener, err = net.Listen("tcp", ":")
		Expect(err).ShouldNot(HaveOccurred())

		server = grpc.NewServer()
		RegisterServer(
			server,
			Marshaler,
			WithApplication(
				"<app-key>",
				mstream,
				types,
			),
		)

		go server.Serve(listener)

		conn, err = grpc.Dial(
			listener.Addr().String(),
			grpc.WithInsecure(),
		)
		Expect(err).ShouldNot(HaveOccurred())

		stream = &Stream{
			App:       configkit.MustNewIdentity("<app-name>", "<app-key>"),
			Client:    transportspec.NewEventStreamClient(conn),
			Marshaler: Marshaler,
		}
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

			cur, err := stream.Open(ctx, 0, types)
			if cur != nil {
				cur.Close()
			}
			Expect(err).Should(HaveOccurred())
		})
	})

	Describe("func EventTypes()", func() {
		It("returns an error if the message types can not be queried", func() {
			conn.Close()

			_, err := stream.EventTypes(ctx)
			Expect(err).Should(HaveOccurred())
		})
	})

	Describe("type cursor", func() {
		Describe("func Next()", func() {
			It("returns an error if the server returns an invalid envelope", func() {
				pcl.Envelope.MessageId = ""

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
