package boltpersistence

import (
	"context"
	"os"
	"sync"

	"github.com/dogmatiq/verity/internal/x/bboltx"
	"github.com/dogmatiq/verity/persistence"
	"go.etcd.io/bbolt"
)

// Provider is an implementation of provider.Provider for BoltDB that uses an
// existing open database.
type Provider struct {
	provider

	// DB is the BoltDB database to use.
	DB *bbolt.DB
}

// Open returns a data-store for a specific application.
//
// k is the identity key of the application.
//
// Data stores are opened for exclusive use. If another engine instance has
// already opened this application's data-store, ErrDataStoreLocked is returned.
func (p *Provider) Open(ctx context.Context, k string) (persistence.DataStore, error) {
	return p.open(
		ctx,
		k,
		func() (*bbolt.DB, error) {
			return p.DB, nil
		},
		func(*bbolt.DB) error {
			// Don't actually close the database, since we didn't open it.
			return nil
		},
	)
}

// FileProvider is an implementation of provider.Provider for BoltDB that opens
// a BoltDB database file.
type FileProvider struct {
	provider

	// Path is the path to the BoltDB database to open or create.
	Path string

	// Mode is the file mode for the created file.
	// If it is zero, 0600 (owner read/write only) is used.
	Mode os.FileMode

	// Options is the BoltDB options for the database.
	// If it is nil, bbolt.DefaultOptions is used.
	Options *bbolt.Options
}

// Open returns a data-store for a specific application.
//
// k is the identity key of the application.
//
// Data stores are opened for exclusive use. If another engine instance has
// already opened this application's data-store, ErrDataStoreLocked is returned.
func (p *FileProvider) Open(ctx context.Context, k string) (persistence.DataStore, error) {
	return p.open(
		ctx,
		k,
		func() (*bbolt.DB, error) {
			return bboltx.Open(ctx, p.Path, p.Mode, p.Options)
		},
		func(db *bbolt.DB) error {
			return db.Close()
		},
	)
}

// provider is the common implementation of Provider and FileProvider.
type provider struct {
	m     sync.Mutex
	db    *bbolt.DB
	close func(db *bbolt.DB) error
	apps  map[string]struct{}
}

// open returns a data-store for a specific application.
func (p *provider) open(
	_ context.Context,
	k string,
	open func() (*bbolt.DB, error),
	close func(db *bbolt.DB) error,
) (persistence.DataStore, error) {
	p.m.Lock()
	defer p.m.Unlock()

	if p.db == nil {
		db, err := open()
		if err != nil {
			return nil, err
		}

		p.db = db
		p.close = close
	}

	if p.apps == nil {
		p.apps = map[string]struct{}{}
	} else if _, ok := p.apps[k]; ok {
		return nil, persistence.ErrDataStoreLocked
	}

	p.apps[k] = struct{}{}

	return &dataStore{
		db:      p.db,
		appKey:  []byte(k),
		release: p.release,
	}, nil
}

// release marks a previously-opened data-store as closed, releasing the lock on
// that application.
func (p *provider) release(k string) error {
	p.m.Lock()
	defer p.m.Unlock()

	delete(p.apps, k)

	if len(p.apps) > 0 {
		return nil
	}

	db := p.db
	close := p.close

	p.db = nil
	p.close = nil

	return close(db)
}
