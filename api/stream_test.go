package api_test

import (
	"context"
	"net"
	"time"

	. "github.com/dogmatiq/configkit/fixtures"
	"github.com/dogmatiq/configkit/message"
	. "github.com/dogmatiq/dogma/fixtures"
	. "github.com/dogmatiq/infix/api"
	. "github.com/dogmatiq/infix/fixtures"
	"github.com/dogmatiq/infix/persistence"
	"github.com/dogmatiq/infix/persistence/memory"
	. "github.com/dogmatiq/marshalkit/fixtures"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"google.golang.org/grpc"
)

var _ = Describe("type stream", func() {
	var (
		ctx      context.Context
		cancel   func()
		listener net.Listener
		gserver  *grpc.Server
		source   *memory.Stream
		stream   persistence.Stream
		types    message.TypeSet

		env1 = NewEnvelope("<message-1>", MessageA1)
		env2 = NewEnvelope("<message-2>", MessageB1)
		env3 = NewEnvelope("<message-3>", MessageA2)
		env4 = NewEnvelope("<message-4>", MessageB2)
	)

	BeforeEach(func() {
		ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)

		types = message.NewTypeSet(MessageAType, MessageBType)
		source = &memory.Stream{}
		source.Append(env1, env2, env3, env4)

		var err error
		listener, err = net.Listen("tcp", ":")
		Expect(err).ShouldNot(HaveOccurred())

		gserver = grpc.NewServer()
		RegisterEventStreamServer(
			gserver,
			Marshaler,
			map[string]persistence.Stream{
				"<app-key>": source,
			},
		)

		go gserver.Serve(listener)

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

		if gserver != nil {
			gserver.Stop()
		}

		cancel()
	})

	Describe("func Open()", func() {
		It("honours the initial offset", func() {
			cur, err := stream.Open(ctx, 2, types)
			Expect(err).ShouldNot(HaveOccurred())
			defer cur.Close()

			m, err := cur.Next(ctx)
			Expect(err).ShouldNot(HaveOccurred())
			Expect(m).To(Equal(
				&persistence.StreamMessage{
					Offset:   2,
					Envelope: env3,
				},
			))

			m, err = cur.Next(ctx)
			Expect(err).ShouldNot(HaveOccurred())
			Expect(m).To(Equal(
				&persistence.StreamMessage{
					Offset:   3,
					Envelope: env4,
				},
			))
		})

		It("limits results to the supplied message types", func() {
			cur, err := stream.Open(ctx, 0, message.NewTypeSet(MessageAType))
			Expect(err).ShouldNot(HaveOccurred())
			defer cur.Close()

			m, err := cur.Next(ctx)
			Expect(err).ShouldNot(HaveOccurred())
			Expect(m).To(Equal(
				&persistence.StreamMessage{
					Offset:   0,
					Envelope: env1,
				},
			))

			m, err = cur.Next(ctx)
			Expect(err).ShouldNot(HaveOccurred())
			Expect(m).To(Equal(
				&persistence.StreamMessage{
					Offset:   2,
					Envelope: env3,
				},
			))
		})

		It("returns an error if the context is closed", func() {
			cancel()

			_, err := stream.Open(ctx, 0, types)
			Expect(err).Should(HaveOccurred())
		})

		It("returns an error if the making the request fails", func() {
			gserver.Stop()

			_, err := stream.Open(ctx, 0, types)
			Expect(err).Should(HaveOccurred())
		})
	})

	Describe("type cursor", func() {
		Describe("func Next()", func() {
			It("returns an error if the server returns an invalid envelope", func() {
				env1.MetaData.MessageID = ""

				cur, err := stream.Open(ctx, 0, types)
				Expect(err).ShouldNot(HaveOccurred())
				defer cur.Close()

				_, err = cur.Next(ctx)
				Expect(err).Should(HaveOccurred())
			})

			It("returns an error if the cursor is already closed", func() {
				cur, err := stream.Open(ctx, 4, types)
				Expect(err).ShouldNot(HaveOccurred())

				cur.Close()

				_, err = cur.Next(ctx)
				Expect(err).Should(HaveOccurred())
			})

			It("returns an error if the context is already canceled", func() {
				cur, err := stream.Open(ctx, 4, types)
				Expect(err).ShouldNot(HaveOccurred())
				defer cur.Close()

				cancel()

				_, err = cur.Next(ctx)
				Expect(err).Should(HaveOccurred())
			})

			It("returns an error if the cursor is closed while blocked", func() {
				cur, err := stream.Open(ctx, 4, types)
				Expect(err).ShouldNot(HaveOccurred())

				go func() {
					time.Sleep(100 * time.Millisecond)
					cur.Close()
				}()

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

			It("returns an error if the context is canceled while blocked", func() {
				cur, err := stream.Open(ctx, 4, types)
				Expect(err).ShouldNot(HaveOccurred())
				defer cur.Close()

				go func() {
					time.Sleep(100 * time.Millisecond)
					cancel()
				}()

				_, err = cur.Next(ctx)
				Expect(err).Should(HaveOccurred())
			})

			It("returns an error if the server goes away while blocked", func() {
				cur, err := stream.Open(ctx, 4, types)
				Expect(err).ShouldNot(HaveOccurred())
				defer cur.Close()

				go func() {
					time.Sleep(100 * time.Millisecond)
					gserver.Stop()
				}()

				_, err = cur.Next(ctx)
				Expect(err).Should(HaveOccurred())
			})
		})
	})
})
