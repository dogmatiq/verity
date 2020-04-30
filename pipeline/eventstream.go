package pipeline

import (
	"context"

	"github.com/dogmatiq/infix/eventstream"
	"github.com/dogmatiq/infix/eventstream/memorystream"
	"github.com/dogmatiq/infix/parcel"
	"github.com/dogmatiq/infix/persistence/subsystem/eventstore"
)

// AddToEventCache returns an observer that calls c.Add() for each batch of
// events that is recorded.
func AddToEventCache(c *memorystream.Stream) EventObserver {
	return func(
		_ context.Context,
		parcels []*parcel.Parcel,
		items []*eventstore.Item,
	) error {
		events := make([]*eventstream.Event, len(parcels))

		for i, p := range parcels {
			events[i] = &eventstream.Event{
				Offset: items[i].Offset,
				Parcel: p,
			}
		}

		c.Add(events)
		return nil
	}
}
