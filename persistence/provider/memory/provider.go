package memory

import (
	"context"
	"sync"

	"github.com/dogmatiq/configkit"
	"github.com/dogmatiq/infix/persistence"
	"github.com/dogmatiq/marshalkit"
)

// provider is an in-memory implementation of provider.Provider.
type provider struct {
	dataStores sync.Map
}

// New returns a persistence provider that stores data in-memory.
func New() persistence.Provider {
	return &provider{}
}

// Open returns a data-store for a specific application.
func (p *provider) Open(
	ctx context.Context,
	app configkit.Identity,
	_ marshalkit.Marshaler,
) (persistence.DataStore, error) {
	ds, _ := p.dataStores.LoadOrStore(
		app.Key,
		&dataStore{},
	)

	return ds.(persistence.DataStore), nil
}
