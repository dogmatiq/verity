package cache

import (
	"context"
	"sync"
	"time"

	"github.com/dogmatiq/cosyne"
	"github.com/dogmatiq/dodeca/logging"
	"github.com/dogmatiq/infix/handler"
	"github.com/dogmatiq/linger"
)

// DefaultTTL is the default *minimum* period of time to keep cache records in
// memory after they were last used.
const DefaultTTL = 1 * time.Hour

// Cache is an in-memory cache for storing aggregate and process instances for a
// single handler.
type Cache struct {
	// TTL is the *minimum* period of time to keep cache records in memory after
	// they were last used. If it is non-positive, DefaultTTL is used.
	TTL time.Duration

	// Logger is the target for log messages about modifications to the cache.
	Logger logging.Logger

	// records is a map of instance ID to *Record.
	records sync.Map
}

// Record is an entry in the cache.
type Record struct {
	id       string
	m        cosyne.Mutex
	state    state
	Instance interface{} // note: exposed, but still protected by m
}

// Acquire locks and returns the cache record with the given ID, and ties its
// lifetime to w.
//
// The lock on the record is automatically released when w is completed. The
// record is removed from the cache if w fails.
//
// If the record has already been acquired, it blocks until the record is
// released or ctx is canceled.
func (c *Cache) Acquire(
	ctx context.Context,
	w handler.UnitOfWork,
	id string,
) (*Record, error) {
	rec, err := c.acquire(ctx, id)
	if err != nil {
		return nil, err
	}

	// Add an observer that releases the lock on the cache record only after the
	// unit-of-work is complete.
	w.Observe(func(_ handler.Result, err error) {
		if err != nil {
			// The unit-of-work was failed, so we forcibly discard the record.
			// The assumption is that the contents of the record was modified in
			// some way that was not persisted, and so the record is now out of
			// date.
			if c.remove(rec) {
				if logging.IsDebug(c.Logger) {
					logging.Debug(
						c.Logger,
						"cache record %s removed (unit-of-work failed) [%p]",
						rec.id,
						rec,
					)
				}
			}
		}

		rec.m.Unlock()
	})

	return rec, nil
}

// acquire locks and returns the cache record with the given ID.
//
// If the record has already been acquired, it blocks until the record is
// released or ctx is canceled.
func (c *Cache) acquire(ctx context.Context, id string) (*Record, error) {
	for {
		rec := &Record{id: id}

		if x, loaded := c.records.LoadOrStore(id, rec); loaded {
			rec = x.(*Record)
		} else if logging.IsDebug(c.Logger) {
			logging.Debug(
				c.Logger,
				"cache record %s added [%p]",
				id,
				rec,
			)
		}

		if err := rec.m.Lock(ctx); err != nil {
			return nil, err
		}

		switch rec.state {
		case active:
			return rec, nil

		case idle:
			rec.state = active

			if logging.IsDebug(c.Logger) {
				logging.Debug(
					c.Logger,
					"cache record %s marked active [%p]",
					id,
					rec,
				)
			}

			return rec, nil

		case removed:
			// We finally got the lock, but this specific record has been removed
			// from the cache, so we try again, creating a new record if necessary.
			//
			// We still need to unlock the mutex in case there are even more blocked
			// acquirers for this record waiting to find out that they too failed
			// miserably.
			rec.m.Unlock()
		}
	}
}

// Discard forcibly removes an acquired record from the cache.
func (c *Cache) Discard(rec *Record) {
	if c.remove(rec) {
		if logging.IsDebug(c.Logger) {
			logging.Debug(
				c.Logger,
				"cache record %s removed (forcibly discarded) [%p]",
				rec.id,
				rec,
			)
		}
	}
}

// Run manages evicting idle records from the cache until ctx is canceled.
func (c *Cache) Run(ctx context.Context) error {
	for {
		if err := linger.Sleep(ctx, c.TTL, DefaultTTL); err != nil {
			return err
		}

		c.records.Range(
			func(_, x interface{}) bool {
				c.tryEvict(x.(*Record))
				return true
			},
		)
	}
}

// tryEvict attempts to mark rec for eviction (idle), or actually evicts it if
// already marked.
//
// If the record is locked, it does nothing.
func (c *Cache) tryEvict(rec *Record) {
	if !rec.m.TryLock() {
		return
	}
	defer rec.m.Unlock()

	switch rec.state {
	case active:
		// Mark the record as idle, if it's still idle on the next tick we'll
		// remove it.
		rec.state = idle

		if logging.IsDebug(c.Logger) {
			logging.Debug(
				c.Logger,
				"cache record %s marked idle [%p]",
				rec.id,
				rec,
			)
		}

	case idle:
		// It's still idle, meaning it hasn't been acquired since the last tick.
		if c.remove(rec) {
			if logging.IsDebug(c.Logger) {
				logging.Debug(
					c.Logger,
					"cache record %s removed (evicted) [%p]",
					rec.id,
					rec,
				)
			}
		}
	}
}

// remove removes rec from the cache.
//
// It is assumed that rec.m is already locked.
func (c *Cache) remove(rec *Record) bool {
	if rec.state == removed {
		return false
	}

	rec.state = removed
	c.records.Delete(rec.id)

	return true
}

// state is an enumeration that describes a record's state in the cache.
type state int

const (
	// active means that the record is still in the cache, and was created or
	// acquired since the last eviction cycle.
	active state = iota

	// idle means that the record is in the cache, but KeepAlive() has not been
	// called (and hence it has not been acquired) since the last eviction
	// cycle. It will be evicted on the next cycle.
	idle

	// removed means that the record has been removed from the cache and should
	// not be used. Locking the record's mutex does not guarantee exclusive
	// access to the instance.
	removed
)
