package memory

import (
	"sync"
	"sync/atomic"

	"github.com/dogmatiq/configkit/message"
	"github.com/dogmatiq/infix/persistence/eventstore"
)

// database encapsulates a single application's "persisted" data.
type database struct {
	open uint32 // atomic

	m      sync.RWMutex
	events []*event
}

// event is an event "persisted" in the database.
type event struct {
	Type  message.Type
	Event eventstore.Event
}

func (db *database) TryOpen() bool {
	return atomic.CompareAndSwapUint32(&db.open, 0, 1)
}

func (db *database) Close() {
	atomic.CompareAndSwapUint32(&db.open, 1, 0)
}
