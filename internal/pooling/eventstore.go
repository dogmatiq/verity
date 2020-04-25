package pooling

import (
	"sync"

	"github.com/dogmatiq/infix/persistence/subsystem/eventstore"
)

// EventStoreItemSlice is a pool of []*eventstore.Item.
var EventStoreItemSlice eventStoreItemSlice

type eventStoreItemSlice sync.Pool

// Get returns a slice with capacity c.
func (p *eventStoreItemSlice) Get(c int) []*eventstore.Item {
	if v := (*sync.Pool)(p).Get(); v != nil {
		s := v.([]*eventstore.Item)
		if cap(s) >= c {
			return s
		}
	}

	return make([]*eventstore.Item, 0, c)
}

// Put adds v to the pool.
func (p *eventStoreItemSlice) Put(v []*eventstore.Item) {
	(*sync.Pool)(p).Put(v[:0])
}
