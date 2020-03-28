package fixtures

import (
	"context"

	"github.com/dogmatiq/infix/persistence"
	"github.com/dogmatiq/infix/persistence/provider/memory"
)

// PersistenceProvider is a mock of the persistence.Provider interface.
//
// It is based on the memory provider.
type PersistenceProvider struct {
	Memory memory.Provider

	OpenFunc func(context.Context, string) (persistence.DataStore, error)
}

// Open returns a data-store for a specific application.
//
// If p.OpenFunc is non-nil, it returns p.OpenFunc(ctx, k), otherwise it
// dispatches to p.Memory.
func (p *PersistenceProvider) Open(ctx context.Context, k string) (persistence.DataStore, error) {
	if p.OpenFunc != nil {
		return p.OpenFunc(ctx, k)
	}

	return p.Memory.Open(ctx, k)
}
