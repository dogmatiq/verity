package fixtures

import (
	"context"

	"github.com/dogmatiq/infix/persistence"
)

// PersistenceProvider is a test implementation of the persistence.Provider
// interface.
type PersistenceProvider struct {
	persistence.Provider

	OpenFunc func(context.Context, string) (persistence.DataStore, error)
}

// Open returns a data-store for a specific application.
func (p *PersistenceProvider) Open(ctx context.Context, k string) (persistence.DataStore, error) {
	if p.OpenFunc != nil {
		return p.OpenFunc(ctx, k)
	}

	if p.Provider != nil {
		return p.Provider.Open(ctx, k)
	}

	return nil, nil
}
