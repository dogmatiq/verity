package boltdb

import (
	"context"
	"os"
	"sync"

	"github.com/dogmatiq/configkit"
	"github.com/dogmatiq/infix/internal/x/bboltx"
	"github.com/dogmatiq/infix/persistence"
	"github.com/dogmatiq/marshalkit"
	"go.etcd.io/bbolt"
)

// provider is a implementation of provider.Provider for BoltDB that uses an
// existing open database.
type provider struct {
	DB *bbolt.DB
}

// New returns a persistence provider that uses the given BoltDB database.
func New(db *bbolt.DB) persistence.Provider {
	return &provider{db}
}

// Open returns a data-store for a specific application.
func (p *provider) Open(
	ctx context.Context,
	app configkit.Identity,
	m marshalkit.Marshaler,
) (persistence.DataStore, error) {
	return &dataStore{
		App:       app,
		Marshaler: m,
		DB:        p.DB,
	}, nil
}

// opener is a implementation of provider.Provider for BoltDB that opens a
// BoltDB database file.
type opener struct {
	Path    string
	Mode    os.FileMode
	Options *bbolt.Options

	m    sync.Mutex
	refs int64
	db   *bbolt.DB
}

// NewOpener returns a persistence provider that opens a BoltDB database.
func NewOpener(
	path string,
	mode os.FileMode,
	options *bbolt.Options,
) persistence.Provider {
	return &opener{
		Path:    path,
		Mode:    mode,
		Options: options,
	}
}

// Open returns a data-store for a specific application.
func (p *opener) Open(
	ctx context.Context,
	app configkit.Identity,
	m marshalkit.Marshaler,
) (persistence.DataStore, error) {
	p.m.Lock()
	defer p.m.Unlock()

	if p.refs == 0 {
		var err error
		p.db, err = bboltx.Open(ctx, p.Path, p.Mode, p.Options)
		if err != nil {
			return nil, err
		}
	}

	p.refs++

	return &dataStore{
		App:       app,
		Marshaler: m,
		DB:        p.db,
		Closer:    p.close,
	}, nil
}

func (p *opener) close() error {
	p.m.Lock()
	defer p.m.Unlock()

	p.refs--

	if p.refs == 0 {
		return p.db.Close()
	}

	return nil
}
