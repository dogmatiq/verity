package memory

import (
	"context"
	"sync"

	"github.com/dogmatiq/infix/persistence"
)

// Provider is an implementation of persistence.Provider that stores application
// data in memory.
type Provider struct {
	m    sync.Mutex
	data map[string]*data
}

// Open returns a data-store for a specific application.
//
// k is the identity key of the application.
//
// Data stores are opened for exclusive use. If another engine instance has
// already opened this application's data-store, ErrDataStoreLocked is
// returned.
func (p *Provider) Open(ctx context.Context, k string) (persistence.DataStore, error) {
	p.m.Lock()
	defer p.m.Unlock()

	d, ok := p.data[k]

	if !ok {
		if p.data == nil {
			p.data = map[string]*data{}
		}

		d = &data{}
		p.data[k] = d
	}

	if d.TryLock() {
		return &dataStore{data: d}, nil
	}

	return nil, persistence.ErrDataStoreLocked
}
