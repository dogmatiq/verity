package eventbuffer

import (
	"context"
	"errors"
	"sync"

	"github.com/dogmatiq/dodeca/logging"
	"github.com/dogmatiq/infix/internal/x/containerx/pqueue"
	"github.com/dogmatiq/infix/parcel"
	"github.com/dogmatiq/infix/persistence/subsystem/eventstore"
)

// DefaultSize is the default number of recent events to buffer in memory.
const DefaultSize = 100

// Buffer is an in-memory buffer of recently recorded events.
type Buffer struct {
	// NextOffset is the offset of the next event to be recorded. This value is
	// not updated as time goes on, it is simply used to "seed" the buffer with
	// this information.
	NextOffset eventstore.Offset

	// Size is the maximum number of messages to buffer in memory.
	// If it is non-positive, DefaultSize is used.
	Size int

	// Logger is the target for log messages from the buffer.
	// If it is nil, logging.DefaultLogger is used.
	Logger logging.Logger

	once    sync.Once
	m       sync.RWMutex
	reorder pqueue.Queue      // buffer of slices that arrived out-of-order
	ring    []*parcel.Parcel  // ring-buffer of recent history
	cursor  int               // index into ring where the next event is inserted
	begin   eventstore.Offset // lowest offset in the ring, inclusive
	end     eventstore.Offset // highest offset in the ring, exclusive, aka "next"
	next    *future           // fulfilled when the parcel at "end" is added
}

type elem struct {
	begin   eventstore.Offset
	parcels []*parcel.Parcel
}

func (e *elem) Less(v pqueue.Elem) bool {
	return e.begin < v.(*elem).begin
}

// Get returns a parcel containing the event at a specific offset.
//
// If the event at the given offset is in the buffer ok is true and p is the
// event's parcel.
//
// If o is later than the latest offset known to the buffer, Get() blocks until
// the event with that offset is added to the buffer, or ctx is canceled.
//
// If o is earlier than the earliest offset in the buffer ok is false and p is
// nil. The caller must load the event directly from the event store.
func (b *Buffer) Get(
	ctx context.Context,
	o eventstore.Offset,
) (p *parcel.Parcel, ok bool, err error) {
	b.init()

	b.m.RLock()
	p, next, err := b.get(o)
	b.m.RUnlock()

	if next != nil {
		p, err = next.await(ctx)
	}

	return p, p != nil, err
}

func (b *Buffer) get(o eventstore.Offset) (*parcel.Parcel, *future, error) {
	if o == b.end {
		// The requested offset is the next offset we expect to be recorded.
		// This should be the common case as consumers catch up to the end of
		// the stream.
		return nil, b.next, nil
	}

	if o < b.begin {
		// The requested offset is old enough that it's no longer in the buffer,
		// the consumer should load the event from the store.
		return nil, nil, nil
	}

	if o > b.end {
		// The requested offset is after next offset we expect to be recorded -
		// how did a consumer come to expect this event without having seen the
		// "next" one?
		return nil, nil, errors.New("requested offset is in the future")
	}

	index := b.cursor - int(b.end-o)
	if index < 0 {
		index += len(b.ring)
	}

	return b.ring[index], nil, nil
}

// Add adds events to the buffer.
//
// Note: The buffer takes ownership of the parcels slice.
func (b *Buffer) Add(begin eventstore.Offset, parcels []*parcel.Parcel) {
	b.init()

	b.m.Lock()
	defer b.m.Unlock()

	if begin > b.end {
		logging.Debug(
			b.Logger,
			"deferring %d out-of-order event(s) beginning at offset %d (begin: %d, end: %d)",
			len(parcels),
			begin,
			b.begin,
			b.end,
		)

		b.reorder.Push(&elem{
			begin:   begin,
			parcels: parcels,
		})

		return
	}

	if begin < b.end {
		logging.Debug(
			b.Logger,
			"discarding %d old event(s) beginning at offset %d (begin: %d, end: %d)",
			len(parcels),
			begin,
			b.begin,
			b.end,
		)

		return
	}

	logging.Debug(
		b.Logger,
		"adding %d event(s) and waking any blocked consumers (begin: %d, end: %d)",
		len(parcels),
		b.begin,
		b.end,
	)

	b.next.resolve(parcels[0])
	b.next = &future{}

	ok := true
	for ok {
		b.addBatch(parcels)
		parcels, ok = b.nextBatch()
	}
}

func (b *Buffer) addBatch(parcels []*parcel.Parcel) {
	var (
		capacity = len(b.ring)
		count    = len(parcels)
	)

	b.end += eventstore.Offset(count)

	if n := count - capacity; n >= 0 {
		// For whatever strange reason the buffer is configured with a buffer
		// size so small that we've got enough events in this single batch to
		// fill the entire ring. We just replace the ring wholesale with the
		// tail of the batch.
		b.ring = parcels[n:]
		b.cursor = 0
		b.begin = b.end - eventstore.Offset(capacity)

		logging.Debug(
			b.Logger,
			"replaced entire buffer with %d event(s) from the batch (begin: %d, end: %d)",
			capacity,
			b.begin,
			b.end,
		)

		return
	}

	if n := int(b.end-b.begin) - capacity; n > 0 {
		// The buffer would become too big after these events are added, which
		// means they will "wrap" around the ring and overwrite our oldest
		// events. We have to adjust b.begin to match.
		b.begin += eventstore.Offset(n)

		logging.Debug(
			b.Logger,
			"dropped %d old event(s) from the buffer (begin: %d, end: %d)",
			n,
			b.begin,
			b.end,
		)
	}

	// Copy the parcels into the ring buffer. First the start of the batch over
	// the second half of the ring, then we copy the remainder over the first
	// half of the ring.
	copy(b.ring[b.cursor:], parcels)
	if n := capacity - b.cursor; count > n {
		copy(b.ring, parcels[n:])
	}

	// Finally, we adjust the cursor to the be the index where the next parcel
	// will be inserted, wrapping back to zero if it exceeds the end of the ring.
	b.cursor += count
	if b.cursor >= capacity {
		b.cursor -= capacity
	}
}

// nextBatch pops the next batch from the reorder queue if it's become the
// "next" expected set of events.
func (b *Buffer) nextBatch() ([]*parcel.Parcel, bool) {
	if e, ok := b.reorder.Peek(); ok {
		e := e.(*elem)
		if e.begin == b.end {
			logging.Debug(
				b.Logger,
				"adding %d event(s) from out-of-order batch (begin: %d, end: %d)",
				len(e.parcels),
				b.begin,
				b.end,
			)

			b.reorder.Pop()
			return e.parcels, true
		}
	}

	return nil, false
}

func (b *Buffer) init() {
	b.once.Do(func() {
		size := b.Size
		if size <= 0 {
			size = DefaultSize
		}

		b.ring = make([]*parcel.Parcel, size)
		b.begin = b.NextOffset
		b.end = b.NextOffset
		b.next = &future{}
	})
}
