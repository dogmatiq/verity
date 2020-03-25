package boltdb

import (
	"context"
	"os"
	"sync"

	"github.com/dogmatiq/infix/internal/x/bboltx"
	"github.com/dogmatiq/infix/persistence"
	"go.etcd.io/bbolt"
)

// Provider is an implementation of provider.Provider for BoltDB that uses an
// existing open database.
type Provider struct {
	// DB is the BoltDB database to use.
	DB *bbolt.DB

	open sync.Map
}

// Open returns a data-store for a specific application.
//
// k is the identity key of the application.
//
// Data stores are opened for exclusive use. If another engine instance has
// already opened this application's data-store, ErrDataStoreLocked is returned.
func (p *Provider) Open(ctx context.Context, k string) (persistence.DataStore, error) {
	if _, locked := p.open.LoadOrStore(k, nil); locked {
		return nil, persistence.ErrDataStoreLocked
	}

	return newDataStore(
		p.DB,
		k,
		func() error {
			p.open.Delete(k)
			return nil
		},
	), nil
}

// FileProvider is an implementation of provider.Provider for BoltDB that opens
// a BoltDB database file.
type FileProvider struct {
	// Path is the path to the BoltDB database to open or create.
	Path string

	// Mode is the file mode for the created file.
	// If it is zero, 0600 (owner read/write only) is used.
	Mode os.FileMode

	// Options is the BoltDB options for the database.
	// If it is nil, bbolt.DefaultOptions is used.
	Options *bbolt.Options

	m    sync.Mutex
	db   *bbolt.DB
	open map[string]struct{}
}

// Open returns a data-store for a specific application.
//
// k is the identity key of the application.
//
// Data stores are opened for exclusive use. If another engine instance has
// already opened this application's data-store, ErrDataStoreLocked is returned.
func (p *FileProvider) Open(ctx context.Context, k string) (persistence.DataStore, error) {
	p.m.Lock()
	defer p.m.Unlock()

	if len(p.open) == 0 {
		var err error
		p.db, err = bboltx.Open(ctx, p.Path, p.Mode, p.Options)
		if err != nil {
			return nil, err
		}
	}

	if p.open == nil {
		p.open = map[string]struct{}{}
	} else if _, ok := p.open[k]; ok {
		return nil, persistence.ErrDataStoreLocked
	}

	p.open[k] = struct{}{}

	return newDataStore(
		p.db,
		k,
		func() error {
			return p.close(k)
		},
	), nil
}

func (p *FileProvider) close(k string) error {
	p.m.Lock()
	defer p.m.Unlock()

	delete(p.open, k)

	if len(p.open) > 0 {
		return nil
	}

	db := p.db
	p.db = nil

	return db.Close()
}
