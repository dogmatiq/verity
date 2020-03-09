package memory_test

import (
	"context"
	"time"

	. "github.com/dogmatiq/configkit/fixtures"
	"github.com/dogmatiq/configkit/message"
	. "github.com/dogmatiq/dogma/fixtures"
	"github.com/dogmatiq/infix/envelope"
	"github.com/dogmatiq/infix/persistence"
	. "github.com/dogmatiq/infix/persistence/memory"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"golang.org/x/sync/errgroup"
)

var _ = Describe("type Stream", func() {
	var (
		ctx    context.Context
		cancel func()
		stream *Stream
		types  message.TypeSet
	)

	BeforeEach(func() {
		ctx, cancel = context.WithTimeout(context.Background(), 3*time.Second)
		stream = &Stream{}

		types = message.NewTypeSet(MessageAType, MessageBType)

		stream.Append(
			&envelope.Envelope{Message: MessageA1},
			&envelope.Envelope{Message: MessageB1},
			&envelope.Envelope{Message: MessageA2},
			&envelope.Envelope{Message: MessageB2},
		)
	})

	AfterEach(func() {
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
					Envelope: &envelope.Envelope{Message: MessageA2},
				},
			))

			m, err = cur.Next(ctx)
			Expect(err).ShouldNot(HaveOccurred())
			Expect(m).To(Equal(
				&persistence.StreamMessage{
					Offset:   3,
					Envelope: &envelope.Envelope{Message: MessageB2},
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
					Envelope: &envelope.Envelope{Message: MessageA1},
				},
			))

			m, err = cur.Next(ctx)
			Expect(err).ShouldNot(HaveOccurred())
			Expect(m).To(Equal(
				&persistence.StreamMessage{
					Offset:   2,
					Envelope: &envelope.Envelope{Message: MessageA2},
				},
			))
		})
	})

	Describe("func Append()", func() {
		It("wakes all blocked cursors", func() {
			cursors := 3 // number of cursors to open

			// barrier is used to delay the main (appending) goroutine until the
			// cursors have started blocking.
			barrier := make(chan struct{}, cursors)

			g, ctx := errgroup.WithContext(ctx)

			// start the cursors
			for i := 0; i < cursors; i++ {
				g.Go(func() error {
					defer GinkgoRecover()

					cur, err := stream.Open(ctx, 4, types)
					if err != nil {
						return err
					}
					defer cur.Close()

					barrier <- struct{}{}
					m, err := cur.Next(ctx)
					if err != nil {
						return err
					}

					Expect(m).To(Equal(
						&persistence.StreamMessage{
							Offset:   4,
							Envelope: &envelope.Envelope{Message: MessageA3},
						},
					))

					return nil
				})
			}

			// wait for the cursors to signal they are about to block
			for i := 0; i < cursors; i++ {
				select {
				case <-barrier:
				case <-ctx.Done():
					Expect(ctx.Err()).ShouldNot(HaveOccurred())
				}
			}

			// add a little delay to ensure the cursors actually start blocking
			time.Sleep(100 * time.Millisecond)

			// wake the consumers
			stream.Append(
				&envelope.Envelope{Message: MessageA3},
			)

			err := g.Wait()
			Expect(err).ShouldNot(HaveOccurred())
		})
	})

	Describe("type cursor", func() {
		Describe("func Next()", func() {
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
		})
	})
})
