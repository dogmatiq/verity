package cache

import (
	"context"
	"sync"
	"time"

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

	records sync.Map
}

// Acquire locks and returns the cache record with the given ID.
//
// If the record has already been acquired, it blocks until the record is
// released or ctx is canceled.
func (c *Cache) Acquire(ctx context.Context, id string) (*Record, error) {
	for {
		rec := &Record{
			id:    id,
			cache: c,
		}

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

		if rec.state != removed {
			return rec, nil
		}

		// We finally got the lock, but this specific record has been removed
		// from the cache, so we try again, creating a new record if necessary.
		//
		// We still need to unlock the mutex in case there are even more blocked
		// acquirers for this record waiting to find out that they too failed
		// miserably.
		rec.m.Unlock()
	}
}

// AcquireForUnitOfWork locks and returns the cache record with the given ID,
// and ties its lifetime to w.
//
// The record is automatically released when w is completed. It is removed from
// the cache if w fails.
//
// If the record has already been acquired, it blocks until the record is
// released or ctx is canceled.
func (c *Cache) AcquireForUnitOfWork(
	ctx context.Context,
	w handler.UnitOfWork,
	id string,
) (*Record, error) {
	rec, err := c.Acquire(ctx, id)
	if err != nil {
		return nil, err
	}

	// Add an observer that releases the lock on the cache record only after the
	// unit-of-work is complete.
	w.Observe(func(_ handler.Result, err error) {
		if err == nil {
			// The unit-of-work was persisted successfully, so we call
			// KeepAlive() to ensure rec stays in the cache when it is released.
			//
			// If the unit-of-work failed to persist, we have to discard the
			// cached instance, because it is no longer in sync with what's
			// persisted.
			rec.KeepAlive()
		}

		rec.Release()
	})

	return rec, nil
}

// Run manages evicting idle records from the cache until ctx is canceled.
func (c *Cache) Run(ctx context.Context) error {
	for {
		if err := linger.Sleep(ctx, c.TTL, DefaultTTL); err != nil {
			return err
		}

		c.records.Range(
			func(_, x interface{}) bool {
				rec := x.(*Record)
				rec.evict()
				return true
			},
		)
	}
}
