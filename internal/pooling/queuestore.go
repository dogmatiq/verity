package pooling

import (
	"sync"

	"github.com/dogmatiq/infix/persistence/subsystem/queuestore"
)

// QueueStoreItemSlice is a pool of []*queuestore.Item.
var QueueStoreItemSlice queueStoreItemSlice

type queueStoreItemSlice sync.Pool

// Get returns a slice with capacity c.
func (p *queueStoreItemSlice) Get(c int) []*queuestore.Item {
	if v := (*sync.Pool)(p).Get(); v != nil {
		s := v.([]*queuestore.Item)
		if cap(s) >= c {
			return s
		}
	}

	return make([]*queuestore.Item, 0, c)
}

// Put adds v to the pool.
func (p *queueStoreItemSlice) Put(v []*queuestore.Item) {
	(*sync.Pool)(p).Put(v[:0])
}
