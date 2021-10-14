package process

import (
	"context"

	"github.com/dogmatiq/dogma"
	"github.com/dogmatiq/marshalkit"
	"github.com/dogmatiq/verity/persistence"
)

// Instance is an in-memory representation of the process instance, as stored
// in the cache.
type Instance struct {
	persistence.ProcessInstance
	Root dogma.ProcessRoot
}

// Loader loads aggregate instances from their historical events.
type Loader struct {
	// Repository is the repository used to load process instances.
	Repository persistence.ProcessRepository

	// Marshaler is used to marshal/unmarshal process instances.
	Marshaler marshalkit.ValueMarshaler
}

// Load loads the aggregate instance with the given ID.
func (l *Loader) Load(
	ctx context.Context,
	hk, id string,
	base dogma.ProcessRoot,
) (*Instance, error) {
	persisted, err := l.Repository.LoadProcessInstance(ctx, hk, id)
	if err != nil {
		return nil, err
	}

	inst := &Instance{
		ProcessInstance: persisted,
	}

	if inst.Revision == 0 {
		inst.Root = base
		return inst, nil
	}

	// An empty packet represents a stateless process root.
	if persisted.Packet.MediaType == "" && len(persisted.Packet.Data) == 0 {
		inst.Root = dogma.StatelessProcessRoot
		return inst, nil
	}

	inst.Root, err = l.Marshaler.Unmarshal(persisted.Packet)

	return inst, err
}
