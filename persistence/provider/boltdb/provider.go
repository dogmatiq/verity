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

// Provider is an implementation of provider.Provider for BoltDB that uses an
// existing open database.
type Provider struct {
	// DB is the BoltDB database to use.
	DB *bbolt.DB
}

// Open returns a data-store for a specific application.
func (p *Provider) Open(
	ctx context.Context,
	cfg configkit.RichApplication,
	m marshalkit.Marshaler,
) (persistence.DataStore, error) {
	return &dataStore{
		App:       cfg.Identity(),
		Marshaler: m,
		DB:        p.DB,
	}, nil
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
	refs int64
	db   *bbolt.DB
}

// Open returns a data-store for a specific application.
func (p *FileProvider) Open(
	ctx context.Context,
	cfg configkit.RichApplication,
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
		App:       cfg.Identity(),
		Marshaler: m,
		DB:        p.db,
		Closer:    p.close,
	}, nil
}

func (p *FileProvider) close() error {
	p.m.Lock()
	defer p.m.Unlock()

	p.refs--

	if p.refs == 0 {
		return p.db.Close()
	}

	return nil
}
