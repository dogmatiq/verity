package sql

import (
	"context"
	"database/sql"
	"runtime"
	"sync"
	"time"

	"github.com/dogmatiq/configkit"
	"github.com/dogmatiq/infix/persistence"
	"github.com/dogmatiq/linger"
	"github.com/dogmatiq/linger/backoff"
	"github.com/dogmatiq/marshalkit"
)

var (
	// DefaultStreamBackoff is the default backoff strategy for stream polling.
	DefaultStreamBackoff backoff.Strategy = backoff.WithTransforms(
		backoff.Exponential(10*time.Millisecond),
		linger.FullJitter,
		linger.Limiter(0, 5*time.Second),
	)

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
	// DB is the SQL database to use.
	DB *sql.DB

	// Driver is the Infix SQL driver to use with this database.
	// If it is nil, it is determined automatically for built-in drivers.
	Driver *Driver

	// StreamBackoff is the backoff strategy used to determine delays betweens
	// stream polls that do not produce any results.
	//
	// If it is nil, DefaultStreamBackoff is used.
	StreamBackoff backoff.Strategy
}

// Open returns a data-store for a specific application.
func (p *Provider) Open(
	ctx context.Context,
	cfg configkit.RichApplication,
	m marshalkit.Marshaler,
) (persistence.DataStore, error) {
	d := p.Driver
	if d == nil {
		var err error
		d, err = NewDriver(p.DB)
		if err != nil {
			return nil, err
		}
	}

	return newDataStore(
		cfg,
		m,
		p.DB,
		d,
		p.StreamBackoff,
		nil,
	), nil
}

// DSNProvider is an implementation of provider.Provider for SQL that opens a
// a database pool using a DSN.
type DSNProvider struct {
	// DriverName is the driver name to be passed to sql.Open().
	DriverName string

	// DSN is the data-source name to be passed to sql.Open().
	DSN string

	// Driver is the Infix SQL driver to use with this database.
	// If it is nil, it is determined automatically for built-in drivers.
	Driver *Driver

	// StreamBackoff is the backoff strategy used to determine delays betweens
	// stream polls that do not produce any results.
	//
	// If it is nil, DefaultStreamBackoff is used.
	StreamBackoff backoff.Strategy

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

	m      sync.Mutex
	refs   int64
	db     *sql.DB
	driver *Driver
}

// Open returns a data-store for a specific application.
func (p *DSNProvider) Open(
	ctx context.Context,
	cfg configkit.RichApplication,
	m marshalkit.Marshaler,
) (persistence.DataStore, error) {
	p.m.Lock()
	defer p.m.Unlock()

	if p.refs == 0 {
		if err := p.open(); err != nil {
			return nil, err
		}
	}

	p.refs++

	return newDataStore(
		cfg,
		m,
		p.db,
		p.driver,
		p.StreamBackoff,
		p.close,
	), nil
}

func (p *DSNProvider) open() (err error) {
	p.db, err = sql.Open(p.DriverName, p.DSN)
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			p.db.Close()
		}
	}()

	p.driver = p.Driver
	if p.driver == nil {
		p.driver, err = NewDriver(p.db)
		if err != nil {
			return err
		}
	}

	idle := p.MaxIdleConns
	if idle == 0 {
		idle = DefaultMaxIdleConns
	}
	p.db.SetMaxIdleConns(idle)

	open := p.MaxOpenConns
	if open == 0 {
		open = DefaultMaxOpenConns
	}
	p.db.SetMaxOpenConns(open)

	ttl := p.MaxConnLifetime
	if ttl == 0 {
		ttl = DefaultMaxConnLifetime
	}
	p.db.SetConnMaxLifetime(ttl)

	return nil
}

func (p *DSNProvider) close() error {
	p.m.Lock()
	defer p.m.Unlock()

	p.refs--

	if p.refs == 0 {
		db := p.db
		p.db = nil
		p.driver = nil
		return db.Close()
	}

	return nil
}
