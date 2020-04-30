package pipeline_test

import (
	"context"
	"time"

	"github.com/dogmatiq/configkit"
	"github.com/dogmatiq/configkit/message"
	. "github.com/dogmatiq/dogma/fixtures"
	"github.com/dogmatiq/infix/eventstream"
	"github.com/dogmatiq/infix/eventstream/memorystream"
	. "github.com/dogmatiq/infix/fixtures"
	"github.com/dogmatiq/infix/parcel"
	"github.com/dogmatiq/infix/persistence/subsystem/eventstore"
	"github.com/dogmatiq/infix/pipeline"
	. "github.com/dogmatiq/infix/pipeline"
	. "github.com/jmalloc/gomegax"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("func AddToEventCache()", func() {
	var (
		ctx      context.Context
		cancel   context.CancelFunc
		stream   *memorystream.Stream
		observer pipeline.EventObserver
		pcl      *parcel.Parcel
		item     *eventstore.Item
	)

	BeforeEach(func() {
		ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)

		pcl = NewParcel("<id>", MessageA1)

		stream = &memorystream.Stream{
			App:         configkit.MustNewIdentity("<app-key>", "<app-name>"),
			Types:       message.TypesOf(MessageA{}),
			FirstOffset: 123,
		}

		observer = AddToEventCache(stream)

		item = &eventstore.Item{
			Offset:   123,
			Envelope: pcl.Envelope,
		}
	})

	AfterEach(func() {
		cancel()
	})

	It("caches events when they are recorded", func() {
		err := observer(
			ctx,
			[]*parcel.Parcel{pcl},
			[]*eventstore.Item{item},
		)
		Expect(err).ShouldNot(HaveOccurred())

		cur, err := stream.Open(ctx, 123, stream.Types)
		Expect(err).ShouldNot(HaveOccurred())
		defer cur.Close()

		ev, err := cur.Next(ctx)
		Expect(err).ShouldNot(HaveOccurred())

		Expect(ev).To(EqualX(
			&eventstream.Event{
				Offset: 123,
				Parcel: pcl,
			},
		))
	})
})
