package networkstream_test

import (
	"context"
	"net"
	"time"

	"github.com/dogmatiq/enginekit/collections/sets"
	. "github.com/dogmatiq/enginekit/enginetest/stubs"
	"github.com/dogmatiq/enginekit/message"
	"github.com/dogmatiq/enginekit/protobuf/identitypb"
	"github.com/dogmatiq/interopspec/eventstreamspec"
	"github.com/dogmatiq/verity/eventstream/internal/streamtest"
	"github.com/dogmatiq/verity/eventstream/memorystream"
	. "github.com/dogmatiq/verity/eventstream/networkstream"
	. "github.com/dogmatiq/verity/fixtures"
	"github.com/dogmatiq/verity/parcel"
	. "github.com/onsi/ginkgo/v2"
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
				WithApplication(
					DefaultAppKey,
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
					App:    in.Application.Identity(),
					Client: eventstreamspec.NewStreamAPIClient(conn),
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
		mstream  *memorystream.Stream
		listener net.Listener
		server   *grpc.Server
		conn     *grpc.ClientConn
		stream   *Stream
		types    *sets.Set[message.Type]
		pcl      parcel.Parcel
	)

	BeforeEach(func() {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
		DeferCleanup(cancel)

		mstream = &memorystream.Stream{}

		pcl = NewParcel("<message-1>", EventA1)
		types = sets.New(
			message.TypeOf(EventA1),
		)

		var err error
		listener, err = net.Listen("tcp", ":")
		Expect(err).ShouldNot(HaveOccurred())
		DeferCleanup(func() { listener.Close() })

		server = grpc.NewServer()
		RegisterServer(
			server,
			WithApplication(
				DefaultAppKey,
				mstream,
				types,
			),
		)

		go server.Serve(listener)
		DeferCleanup(server.Stop)

		conn, err = grpc.Dial(
			listener.Addr().String(),
			grpc.WithInsecure(),
		)
		Expect(err).ShouldNot(HaveOccurred())
		DeferCleanup(func() { conn.Close() })

		stream = &Stream{
			App:    identitypb.MustParse("<app-name>", DefaultAppKey),
			Client: eventstreamspec.NewStreamAPIClient(conn),
		}
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
				pcl.Envelope.MessageId = nil

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
