package process

import (
	"context"
	"fmt"

	"github.com/dogmatiq/dogma"
	"github.com/dogmatiq/infix/persistence"
	"github.com/dogmatiq/marshalkit"
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
	} else if len(persisted.Packet.Data) == 0 {
		inst.Root = dogma.StatelessAggregateRoot
		return inst, nil
	}

	v, err := l.Marshaler.Unmarshal(persisted.Packet)
	if err != nil {
		return nil, err
	}

	root, ok := v.(dogma.ProcessRoot)
	if !ok {
		return nil, fmt.Errorf(
			"the process root for handler '%s' with ID '%s' has type %T, which does not implement dogma.ProcessRoot",
			hk,
			id,
			v,
		)
	}

	inst.Root = root

	return inst, nil
}
