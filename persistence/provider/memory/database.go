package memory

import (
	"sync"
	"sync/atomic"

	"github.com/dogmatiq/infix/draftspecs/envelopespec"
	"github.com/dogmatiq/infix/persistence"
	"google.golang.org/protobuf/proto"
)

// database encapsulates a single application's data.
type database struct {
	open      uint32 // atomic
	mutex     sync.RWMutex
	aggregate aggregateDatabase
	event     eventDatabase
	offset    offsetDatabase
	queue     queueDatabase
}

// newDatabase returns a new empty database.
func newDatabase() *database {
	return &database{}
}

// TryOpen attempts to open the database. If the database is already open it
// returns false.
//
// This is used to enforce the requirement that persistence providers only allow
// a single open data-store for each application.
func (db *database) TryOpen() bool {
	return atomic.CompareAndSwapUint32(&db.open, 0, 1)
}

// Close closes an open database.
//
// This allows a new data-store for this application to be opened via the
// provider.
func (db *database) Close() {
	atomic.CompareAndSwapUint32(&db.open, 1, 0)
}

// cloneEnvelope returns a deep-clone of env.
func cloneEnvelope(env *envelopespec.Envelope) *envelopespec.Envelope {
	return proto.Clone(env).(*envelopespec.Envelope)
}

// validator is an implementation of persitence.OperationVisitor that
// produces an error if any operations in a batch can not be applied.
type validator struct {
	db *database
}

// committer is an implementation of persitence.OperationVisitor that
// applies operations to the database.
//
// It is expected that the operations have already been validated using
// validator.
type committer struct {
	db     *database
	result persistence.Result
}
