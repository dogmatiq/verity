package sql

import (
	"context"
	"database/sql"
	"runtime"
	"sync"
	"time"

	"github.com/dogmatiq/infix/persistence"
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

	// Driver is the Infix SQL driver to use with this database.
	// If it is nil, it is determined automatically for built-in drivers.
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
		func() (*sql.DB, Driver, error) {
			return p.DB, p.Driver, nil
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

	// Driver is the Infix SQL driver to use with this database.
	// If it is nil, it is determined automatically for built-in drivers.
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
		func() (*sql.DB, Driver, error) {
			db, err := p.openDB()
			return db, p.Driver, err
		},
		func(db *sql.DB) error {
			return db.Close()
		},
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
	close  func(db *sql.DB) error
	apps   map[string]struct{}
}

// open returns a data-store for a specific application.
func (p *provider) open(
	ctx context.Context,
	k string,
	open func() (*sql.DB, Driver, error),
	close func(db *sql.DB) error,
) (persistence.DataStore, error) {
	p.m.Lock()
	defer p.m.Unlock()

	if p.db == nil {
		db, d, err := open()
		if err != nil {
			return nil, err
		}

		if d == nil {
			var err error
			d, err = NewDriver(db)
			if err != nil {
				close(db)
				return nil, err
			}
		}

		p.db = db
		p.driver = d
		p.close = close
	}

	if p.apps == nil {
		p.apps = map[string]struct{}{}
	} else if _, ok := p.apps[k]; ok {
		return nil, persistence.ErrDataStoreLocked
	}

	p.apps[k] = struct{}{}

	return newDataStore(
		p.db,
		p.driver,
		k,
		p.release,
	), nil
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
	p.db = nil

	return p.close(db)
}
