package boltdb

import (
	"context"
	"os"
	"sync/atomic"

	"github.com/dogmatiq/configkit"
	"github.com/dogmatiq/infix/internal/x/bboltx"
	"github.com/dogmatiq/infix/persistence"
	"github.com/dogmatiq/marshalkit"
	"go.etcd.io/bbolt"
)

// Provider is a implementation of provider.Provider for BoltDB.
type Provider struct {
	Path    string
	Mode    os.FileMode
	Options *bbolt.Options

	refs      int64 // atomic
	db        *bbolt.DB
	marshaler marshalkit.Marshaler
}

// Open returns a data-store for a specific application.
func (p *Provider) Open(
	ctx context.Context,
	app configkit.Identity,
	m marshalkit.Marshaler,
) (persistence.DataStore, error) {
	var err error
	p.marshaler = m

	if atomic.AddInt64(&p.refs, 1) == 1 {
		path := p.Path
		if path == "" {
			path = "/var/run/infix.boltdb"
		}

		p.db, err = bboltx.Open(ctx, path, p.Mode, p.Options)
	}

	if err != nil {
		return nil, err
	}

	return &dataStore{
		provider: p,
		stream: &Stream{
			DB:        p.db,
			Marshaler: p.marshaler,
			BucketPath: [][]byte{
				[]byte(app.Key),
				[]byte("eventstream"),
			},
		},
	}, nil
}

type dataStore struct {
	provider *Provider
	stream   *Stream
}

// EventStream returns the event stream for the given application.
func (ds *dataStore) EventStream(_ context.Context) (persistence.Stream, error) {
	return ds.stream, nil
}

// Close closes the data store.
func (ds *dataStore) Close() error {
	if atomic.AddInt64(&ds.provider.refs, -1) == 0 {
		return ds.provider.db.Close()
	}

	return nil
}
