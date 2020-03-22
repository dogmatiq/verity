package memory

import (
	"context"
	"sync"

	"github.com/dogmatiq/configkit"
	"github.com/dogmatiq/infix/persistence"
	"github.com/dogmatiq/marshalkit"
)

// Provider is an in-memory implementation of provider.Provider.
type Provider struct {
	m          sync.Mutex
	dataStores map[string]*dataStore
}

// Open returns a data-store for a specific application.
func (p *Provider) Open(
	ctx context.Context,
	cfg configkit.RichApplication,
	_ marshalkit.Marshaler,
) (persistence.DataStore, error) {
	k := cfg.Identity().Key

	p.m.Lock()
	defer p.m.Unlock()

	if p.dataStores == nil {
		p.dataStores = map[string]*dataStore{}
	} else if ds, ok := p.dataStores[k]; ok {
		return ds, nil
	}

	ds := newDataStore(cfg)
	p.dataStores[k] = ds

	return ds, nil
}
