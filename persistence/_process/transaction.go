package process

import (
	"context"

	"github.com/dogmatiq/infix/persistence/eventstore"
)

// Transaction defines the primitive persistence operations for manipulating
// process state.
type Transaction interface {
	// SaveProcessInstance updates (or creates) a process instance.
	//
	// hk is the identity key of the process message handler. id is the process
	// instance ID.
	//
	// r must be the current revision of the process instance as currently
	// persisted, otherwise an optimistic concurrency conflict has occurred, the
	// process is not saved and ok is false.
	SaveProcessInstance(
		ctx context.Context,
		hk, id string,
		r *Root,
	) (ok bool, err error)

	// DeleteProcessInstance deletes a process instance.
	//
	// hk is the identity key of the process message handler. id is the process
	// instance ID.
	//
	// r must be the current revision of the process instance as currently
	// persisted, otherwise an optimistic concurrency conflict has occurred, the
	// process is not deleted and ok is false.
	DeleteProcessInstance(
		ctx context.Context,
		hk, id string,
		r Revision,
	) (ok bool, err error)

	// SaveProcessStreamOffset records the progress of a process handler through
	// an application's event stream.
	//
	// hk is the identity key of the process message handler. ak is the identity
	// key of the source application.
	//
	// c must be the offset for this application as currently persisted,
	// otherwise an optimistic concurrency conflict has occurred, the offset is
	// not saved and ok is false.
	//
	// o is the offset of the next event to be consumed from the application's
	// stream for this process handler.
	SaveProcessStreamOffset(
		ctx context.Context,
		hk, ak string,
		c, o eventstore.Offset,
	) (ok bool, err error)
}
