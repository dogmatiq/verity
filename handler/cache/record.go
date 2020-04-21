package cache

import "github.com/dogmatiq/infix/internal/x/syncx"

// Record is an entry in the cache.
type Record struct {
	id    string
	cache *Cache

	m        syncx.Mutex
	state    state
	saved    bool
	Instance interface{} // note: exposed, but still protected by m
}

// MarkInstanceSaved marks instance as it appears in the cache record as having
// been successfully persisted.
//
// If this is NOT called, Release() assumes that modifications were made to
// r.Instance that were not persisted successfully, and removes the record from
// the cache.
func (r *Record) MarkInstanceSaved() {
	r.saved = true
	r.cache.records.Delete(r.id)
}

// Release unlocks this record, allowing the key to be acquired by other
// callers.
func (r *Record) Release() {
	if r.saved {
		r.saved = false
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
