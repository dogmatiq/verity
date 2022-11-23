package sqlpersistence

import (
	"context"
	"database/sql"
	"runtime"
	"sync"
	"time"

	"github.com/dogmatiq/verity/persistence"
)

var (
	// DefaultMaxIdleConns is the default maximum number of idle connections
	// allowed in the database pool.
	DefaultMaxIdleConns = runtime.GOMAXPROCS(0)

	// DefaultMaxOpenConns is the default maximum number of open connections
	// allowed in the database pool.
	DefaultMaxOpenConns = DefaultMaxIdleConns * 10

	// DefaultMaxConnLifetime is the default maximum lifetime of database
	// connections.
	DefaultMaxConnLifetime = 10 * time.Minute
)

// Provider is an implementation of provider.Provider for SQL that uses an
// existing open database pool.
type Provider struct {
	provider

	// DB is the SQL database to use.
	DB *sql.DB

	// Driver is the Verity SQL driver to use with this database. If it is nil,
	// it is chosen automatically from one of the built-in drivers.
	Driver Driver
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
		p.Driver,
		func() (*sql.DB, error) {
			return p.DB, nil
		},
		func(db *sql.DB) error {
			// Don't actually close the database, since we didn't open it.
			return nil
		},
	)
}

// DSNProvider is an implementation of provider.Provider for SQL that opens a
// a database pool using a DSN.
type DSNProvider struct {
	provider

	// DriverName is the driver name to be passed to sql.Open().
	DriverName string

	// DSN is the data-source name to be passed to sql.Open().
	DSN string

	// Driver is the Verity SQL driver to use with this database. If it is nil,
	// it is chosen automatically from one of the built-in drivers.
	Driver Driver

	// MaxIdleConnections is the maximum number of idle connections allowed in
	// the database pool.
	//
	// If it is zero, DefaultMaxIdleConns is used.
	MaxIdleConns int

	// MaxOpenConnections is the maximum number of open connections allowed in
	// the database pool.
	//
	// If it is zero, DefaultMaxOpenConns is used.
	MaxOpenConns int

	// maxConnLifetime is the maximum lifetime of database connections.
	// If it is zero, DefaultMaxConnLifetime is used.
	MaxConnLifetime time.Duration
}

// Open returns a data-store for a specific application.
//
// k is the identity key of the application.
//
// Data stores are opened for exclusive use. If another engine instance has
// already opened this application's data-store, ErrDataStoreLocked is returned.
func (p *DSNProvider) Open(ctx context.Context, k string) (persistence.DataStore, error) {
	return p.open(
		ctx,
		k,
		p.Driver,
		p.openDB,
		(*sql.DB).Close,
	)
}

// openDB opens the database pool and configures the limits.
func (p *DSNProvider) openDB() (*sql.DB, error) {
	db, err := sql.Open(p.DriverName, p.DSN)
	if err != nil {
		return nil, err
	}

	idle := p.MaxIdleConns
	if idle == 0 {
		idle = DefaultMaxIdleConns
	}
	db.SetMaxIdleConns(idle)

	open := p.MaxOpenConns
	if open == 0 {
		open = DefaultMaxOpenConns
	}
	db.SetMaxOpenConns(open)

	ttl := p.MaxConnLifetime
	if ttl == 0 {
		ttl = DefaultMaxConnLifetime
	}
	db.SetConnMaxLifetime(ttl)

	return db, nil
}

// provider is the common implementation of Provider and DSNProvider.
type provider struct {
	m      sync.Mutex
	db     *sql.DB
	driver Driver
	refs   int
	close  func(db *sql.DB) error
}

// open returns a data-store for a specific application.
func (p *provider) open(
	ctx context.Context,
	k string,
	d Driver,
	open func() (*sql.DB, error),
	close func(db *sql.DB) error,
) (_ persistence.DataStore, err error) {
	p.m.Lock()
	defer p.m.Unlock()

	if p.db == nil {
		db, err := open()
		if err != nil {
			return nil, err
		}

		if d == nil {
			var err error
			d, err = selectDriver(ctx, db)
			if err != nil {
				// Ignore error from close() and instead report the causal error.
				close(db) // nolint:errcheck
				return nil, err
			}
		}

		p.db = db
		p.driver = d
		p.close = close
	}

	if err := p.driver.PurgeEventFilters(ctx, p.db, k); err != nil {
		return nil, err
	}

	p.refs++

	return newDataStore(
		p.db,
		p.driver,
		k,
		p.release,
	), nil
}

// release releases a reference to the database.
func (p *provider) release() error {
	p.m.Lock()
	defer p.m.Unlock()

	p.refs--

	if p.refs > 0 {
		return nil
	}

	db := p.db
	p.db = nil

	return p.close(db)
}
