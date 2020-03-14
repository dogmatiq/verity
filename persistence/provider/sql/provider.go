package sql

import (
	"context"
	"database/sql"
	"runtime"
	"sync"
	"time"

	"github.com/dogmatiq/configkit"
	"github.com/dogmatiq/infix/persistence"
	"github.com/dogmatiq/infix/persistence/provider/sql/driver"
	"github.com/dogmatiq/linger"
	"github.com/dogmatiq/linger/backoff"
	"github.com/dogmatiq/marshalkit"
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

	// DefaultStreamBackoff is the default backoff strategy for stream polling.
	DefaultStreamBackoff backoff.Strategy = backoff.WithTransforms(
		backoff.Exponential(10*time.Millisecond),
		linger.FullJitter,
		linger.Limiter(0, 5*time.Second),
	)
)

// Provider is an implementation of provider.Provider for SQL that uses an
// existing open database pool.
type Provider struct {
	DB            *sql.DB
	Driver        *driver.Driver
	StreamBackoff backoff.Strategy
}

// Open returns a data-store for a specific application.
func (p *Provider) Open(
	ctx context.Context,
	app configkit.Identity,
	m marshalkit.Marshaler,
) (persistence.DataStore, error) {
	d := p.Driver
	if d == nil {
		var err error
		d, err = driver.New(p.DB)
		if err != nil {
			return nil, err
		}
	}

	return &dataStore{
		App:           app,
		Marshaler:     m,
		DB:            p.DB,
		Driver:        d,
		StreamBackoff: p.StreamBackoff,
	}, nil
}

// DSNProvider is an implementation of provider.Provider for SQL that opens a
// a database pool using a DSN.
type DSNProvider struct {
	DriverName      string
	DSN             string
	Driver          *driver.Driver
	MaxIdleConns    int
	MaxOpenConns    int
	MaxConnLifetime time.Duration
	StreamBackoff   backoff.Strategy

	m      sync.Mutex
	refs   int64
	db     *sql.DB
	driver *driver.Driver
}

// Open returns a data-store for a specific application.
func (p *DSNProvider) Open(
	ctx context.Context,
	app configkit.Identity,
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

	return &dataStore{
		App:           app,
		Marshaler:     m,
		DB:            p.db,
		Driver:        p.driver,
		StreamBackoff: p.StreamBackoff,
		Closer:        p.close,
	}, nil
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
		p.driver, err = driver.New(p.db)
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
