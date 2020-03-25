package memory

import (
	"context"
	"sync"

	"github.com/dogmatiq/infix/persistence"
)

// Provider is an implementation of persistence.Provider that stores application
// data in memory.
type Provider struct {
	m         sync.Mutex
	databases map[string]*database
}

// Open returns a data-store for a specific application.
//
// k is the identity key of the application.
//
// Data stores are opened for exclusive use. If another engine instance has
// already opened this application's data-store, ErrDataStoreLocked is returned.
func (p *Provider) Open(ctx context.Context, k string) (persistence.DataStore, error) {
	p.m.Lock()
	defer p.m.Unlock()

	if p.databases == nil {
		p.databases = map[string]*database{}
	}

	db, ok := p.databases[k]

	if !ok {
		db = &database{}
		p.databases[k] = db
	}

	if db.TryOpen() {
		return &dataStore{db: db}, nil
	}

	return nil, persistence.ErrDataStoreLocked
}
