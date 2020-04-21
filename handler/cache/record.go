package cache

import "github.com/dogmatiq/infix/internal/x/syncx"

// Record is an entry in the cache.
type Record struct {
	id    string
	cache *Cache

	m        syncx.Mutex
	state    state
	keep     bool
	Instance interface{} // note: exposed, but still protected by m
}

// KeepAlive resets the TTL for this record, and instructs the cache to keep
// this record when it is released.
//
// It must be called each time the record is acquired, otherwise the record is
// removed when it is released.
//
// If KeepAlive() is NOT called, the assumption is that some modification was
// made to r.Instance while handling a message that was not persisted
// successfully, and hence the record is now out-of-date.
func (r *Record) KeepAlive() {
	// Some further context: This approach of evict-on-failure obviates the need
	// to clone the instance state in order to "rollback" to the state as it
	// existed before the record was acquired when persisting the updated state
	// fails.
	//
	// Even if we chose to clone in this way, we know limited information about
	// the types used to represent the instance state. Dogma itself places NO
	// requirements on these types, and Infix only requires that they be
	// (un)marshalable to binary data (via marshalkit).
	//
	// It's not uncommon to use marshaling/unmarshaling as a cloning mechanism,
	// but one of the reasons this cache exists in the first place is to avoid
	// costly marshaling in the happy-path. (The other being avoiding the I/O
	// roundtrip to the persistence layer).
	r.keep = true
	r.state = active
}

// Release unlocks this record, allowing the key to be acquired by other
// callers.
//
// If KeepAlive() has not been called since the record was acquired, the record
// is removed from the cache.
func (r *Record) Release() {
	if r.keep {
		r.keep = false // for the next acquirer
	} else {
		r.remove()
	}

	r.m.Unlock()
}

// remove removes r from the cache.
func (r *Record) remove() {
	r.state = removed
	r.cache.records.Delete(r.id)
}

// evict marks the record for eviction (idle), or actually evicts it if it's
// already marked.
func (r *Record) evict() {
	if !r.m.TryLock() {
		return
	}
	defer r.m.Unlock()

	switch r.state {
	case active:
		// Mark the record as idle, if it's still idle on the next
		// tick we'll remove it.
		r.state = idle
	case idle:
		// It's still idle, meaning it hasn't been acquired since
		// the last tick.
		r.remove()
	}
}

// state is an enumeration that describes the records state in the cache.
type state int

const (
	active  state = iota // the record is in the cache, it may be locked or unlocked
	idle                 // the record has been marked for eviction on the next cycle
	removed              // the record has been removed from the cache, and is invalid
)
