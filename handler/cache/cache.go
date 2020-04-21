package cache

import (
	"context"
	"sync"
	"time"

	"github.com/dogmatiq/linger"
)

// DefaultTTL is the default *minimum* period of time to keep cache records in
// memory after they were last acquired.
const DefaultTTL = 1 * time.Hour

// Cache is an in-memory cache for storing aggregate and process instances for a
// single handler.
type Cache struct {
	// TTL is the *minimum* period of time to keep cache records in memory after
	// they were last used. If it is non-positive, DefaultTTL is used.
	TTL time.Duration

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
